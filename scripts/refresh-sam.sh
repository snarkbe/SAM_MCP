#!/bin/bash
#
# refresh-sam.sh — nightly SAM DB rebuild (Docker host).
#
# Primary target: Unraid with the User Scripts plugin.
# Works on any Linux host that has Docker and curl/unzip.
#
# On Unraid: add via User Scripts → Schedule: Custom → 30 4 * * *
# On a plain Linux host: add to crontab with `crontab -e`:
#   30 4 * * *  /path/to/refresh-sam.sh >> /var/log/refresh-sam.log 2>&1
#
# The host needs no Python — the ETL runs inside a throwaway container
# of the same sam-mcp image (which already ships uv + sam-mcp-etl).
#
set -euo pipefail

# ---- config (edit these to match your setup) --------------------------------
APPDATA="/mnt/user/appdata/sam-mcp"   # on plain Linux: e.g. /opt/sam-mcp
CONTAINER="sam-mcp"                    # your container's name
IMAGE="snarkbe/sam-mcp:latest"         # same image the container runs

XML="$APPDATA/xml"
DBDIR="$APPDATA/db"
LOG="$APPDATA/refresh.log"

SAM_BASE='https://www.vas.ehealth.fgov.be/websamcivics/samcivics/download'
XSD=6                                  # current schema version

# CBIP commented-repertoire dump (monthly, ~18th). Set to 0 to skip.
# The script auto-detects the newest French SQL edition off the CBIP page,
# downloads it, and only rebuilds when its contents changed.
ENABLE_CBIP=1
CBIP_PAGE='https://www.cbip.be/fr/download'
CBIP_BASE='https://www.cbip.be/fr/downloads/file?type=EMD&name=/sql4Emd_Fr_'

log() { echo "$(date -Is)  $*" | tee -a "$LOG"; }

mkdir -p "$XML" "$DBDIR"

# ---- 1. is there a new SAM export? -----------------------------------------
ver=$(curl -fsS --max-time 60 "$SAM_BASE/samv2-full-getLastVersion?xsd=$XSD")
last=$(cat "$XML/.last_version" 2>/dev/null || echo 0)

rebuild=0
if [ "$ver" = "$last" ]; then
    log "SAM already at version $ver."
else
    log "New SAM export: $last -> $ver. Downloading FULL export (xsd=$XSD)..."
    zip="$XML/sam-$ver.zip"
    curl -fSL --retry 3 --max-time 1800 -o "$zip" \
        "$SAM_BASE/samv2-download?type=FULL&xsd=$XSD&version=$ver"
    unzip -o "$zip" -d "$XML" >/dev/null
    rm -f "$zip"
    echo "$ver" > "$XML/.last_version"
    rebuild=1
fi

# ---- 2. CBIP dump changed? (monthly) ---------------------------------------
code=""
if [ "$ENABLE_CBIP" = 1 ]; then
    code=$(curl -fsS --max-time 60 "$CBIP_PAGE" \
        | grep -oE 'sql4Emd_Fr_[0-9]{4}[A-Z]' | head -1 | grep -oE '[0-9]{4}[A-Z]' || true)
    if [ -z "$code" ]; then
        log "WARN: could not detect CBIP edition from page — skipping CBIP."
    else
        czip="$XML/cbip-$code.zip"
        if curl -fsSL --max-time 300 -o "$czip" "${CBIP_BASE}${code}.zip"; then
            unzip -o "$czip" -d "$XML" >/dev/null     # yields exportFr.sql
            rm -f "$czip"
            new=$(md5sum "$XML/exportFr.sql" | cut -d' ' -f1)
            old=$(cat "$XML/.cbip_md5" 2>/dev/null || echo)
            if [ "$new" != "$old" ]; then
                echo "$new" > "$XML/.cbip_md5"
                rebuild=1
                log "CBIP edition $code is new — will reload."
            else
                log "CBIP edition $code unchanged."
            fi
        else
            log "WARN: CBIP download failed, keeping existing dump."
        fi
    fi
fi

# ---- 3. rebuild in a throwaway container, then atomic swap -----------------
if [ "$rebuild" = 0 ]; then
    log "No changes — DB left as-is."
    exit 0
fi

log "Building database in a temporary container (10-20 min)..."
etl_args=(--data /xml --db /data/sam.new.db)
# The rebuild wipes the DB, so always reload CBIP if we have the dump.
[ -f "$XML/exportFr.sql" ] && etl_args+=(--with-cbip --cbip-sql /xml/exportFr.sql)

set +e
docker run --rm \
    -v "$XML":/xml \
    -v "$DBDIR":/data \
    "$IMAGE" \
    uv run sam-mcp-etl "${etl_args[@]}"
rc=$?
set -e

# Exit-code map (sam_mcp): 0 = clean; 2 = DB built OK but the CBIP loader hit a
# few row-level errors (unparseable rows in the 3rd-party dump — non-fatal);
# anything else = a real failure. Only a real failure keeps the old DB.
if [ "$rc" = 2 ]; then
    log "WARN: CBIP reported row-level errors (exit 2) — DB built fine, continuing."
elif [ "$rc" != 0 ]; then
    log "ERROR: ETL failed (exit $rc) — keeping the old DB."
    rm -f "$DBDIR/sam.new.db"
    exit 1
fi

# Stop the server so the swap is race-free. sam.db is a WAL database, so a bare
# rename of the main file would orphan the old -wal/-shm next to the NEW db and
# risk a malformed-image error on reopen. The ETL VACUUMs + checkpoints into the
# main file, so sam.new.db is self-contained and the old sidecars are stale —
# remove them rather than let SQLite adopt them.
log "Stopping $CONTAINER for the DB swap..."
docker stop "$CONTAINER" >/dev/null || log "WARN: stop failed — swapping anyway."

mv -f "$DBDIR/sam.new.db" "$DBDIR/sam.db"          # atomic rename, same folder
rm -f "$DBDIR/sam.db-wal" "$DBDIR/sam.db-shm"      # drop stale WAL sidecars
rm -f "$DBDIR/sam.new.db-wal" "$DBDIR/sam.new.db-shm"

log "Starting $CONTAINER..."
docker start "$CONTAINER" >/dev/null || log "WARN: start failed — start it manually."

# ---- 4. prune old XML (keep newest 2 of each prefix) -----------------------
for p in $(ls "$XML"/*.xml 2>/dev/null | sed -E 's#.*/([A-Z]+)-.*#\1#' | sort -u); do
    ls -t "$XML/$p"-*.xml 2>/dev/null | tail -n +3 | xargs -r rm -f
done

log "Done. SAM version $ver, CBIP ${code:-skipped}."
