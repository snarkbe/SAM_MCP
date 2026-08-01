# SAM MCP

Local MCP server that answers questions about Belgian medicines from the
official SAM v2 XML exports (FAGG/AFMPS, eHealth), plus CBIP/BCFI editorial
commentary.

**SAM** = *Source Authentique des Médicaments* (Authentic Source of Medicines)

> **Disclaimer** — This is an **unofficial** MCP server built on publicly available data
> (SAM v2 XML exports from FAGG/AFMPS and the CBIP/BCFI repertoire). It is provided as
> a research and information tool only. The information returned does **not** constitute
> medical advice and is **not** a substitute for the opinion of a qualified healthcare
> professional. Always consult a doctor or pharmacist before making any decision about
> medicines.

## Live instance

A public instance is available at:

```
https://sam.reichert.be/mcp
```

**claude.ai / Claude Desktop** — add it via *Customize → Connectors → + → Add custom connector*, paste the URL above.

**Other MCP clients** — use [`mcp-remote`](https://www.npmjs.com/package/mcp-remote) (requires Node.js):

```json
{
  "mcpServers": {
    "sam": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://sam.reichert.be/mcp"]
    }
  }
}
```

> **No guarantees whatsoever.** This instance runs on a home server, is
> updated on a best-effort basis, and may be down, outdated, or broken at
> any time without notice. It is provided as a convenience for testing and
> exploration only — do not rely on it for production use. Self-host if you
> need stability.

## Examples it can answer

- "What is the dose of *Dafalgan 500*?"
- "Which molecule does *Symbicort* contain?"
- "Which medicines contain *salbutamol*?"
- "What's the CNK 3104965?"
- "Is *Eliquis 5 mg* reimbursed, and at what base price?"
- "What are the synonyms for *calcium pantothenate* in the compounding repertoire?"
- "Which non-medicinal products contain *magnesium*?"
- "Search everything for *vitamine d3* — is it a medicine, a supplement, or both, and who makes it?"
- "Find *Dafalgan* in the CBIP/BCFI repertoire, even without knowing its CNK."
- "Is this Dutch-only brand name actually in the database, in any form?"
- "Which active substances have exactly one CNK on the Belgian market?"
- "How many CNKs does metformin have?"
- "List all proton-pump inhibitors (ATC A02BC) with their CNK count."
- "Which PPIs are sold under only one CNK? Give me the pack details."

## Layout

 | Path | What's there |
 | --- | --- |  
 | `src/sam_mcp/` | Python package — ETL + MCP server. |
 | `db/sam.db` | SQLite database produced by the ETL (gitignored). |
 | `xml/` | SAM v2 XML exports (gitignored — drop the official files here). |
 | `exportFr.sql` | CBIP/BCFI repertoire dump (gitignored). |

Source data lives outside git because it's large and regenerable. Get the
XML from <https://www.vas.ehealth.fgov.be/websamcivics/samcivics/> and the
CBIP dump from <https://www.cbip.be/fr/download>.

## How it works

1. **ETL** — `sam_mcp.etl` streams the SAM XML files into a single SQLite
   file (`sam.db`) using `lxml.iterparse`. Each file is processed
   element-by-element with constant memory, regardless of size.

   | XML file | Contents imported |
   | --- | --- |
   | `REF` | ATC codes, substances, pharmaceutical forms, routes |
   | `AMP` | Medicines, ingredients, packs, CNKs |
   | `VMP` | Virtual Therapeutic Molecules (INN-level groupings) |
   | `RMB` | Reimbursement contexts: base/reference prices, criteria |
   | `NONMEDICINAL` | Dietary supplements and other non-medicinal products |
   | `CMP` | Compounding (magistral) ingredients with multilingual synonyms |
   | `RML` | Reimbursement law hierarchy (legal bases, references, texts) |
   | `IMPP` | Imported medicinal products (compassionate use / shortages) |

   All eight loaders are optional — the ETL completes cleanly if a file is
   absent. `CHAPTERIV` is not yet imported.

2. **Server** — `sam_mcp.server` is a FastMCP server that exposes read-only
   query tools over `sam.db`.

## Data sources

- **SAM v2 XML exports** (FAGG/AFMPS, eHealth) — official regulatory data:
  <https://www.vas.ehealth.fgov.be/websamcivics/samcivics/>
- **CBIP/BCFI repertoire dump** — editorial commentary (chapter intros,
  positioning, prescribing notes): <https://www.cbip.be/fr/download>

Drop the SAM XML files into `xml/` and the CBIP `exportFr.sql` dump into the
repo root before running the ETL.

## Setup

This project is managed with [uv](https://github.com/astral-sh/uv). One
command creates the virtualenv, resolves dependencies (pinned in
`uv.lock`), and installs the package in editable mode so changes to any
`.py` file are picked up without reinstalling:

```bash
uv sync
```

After that, the venv lives in `.venv\` and exposes the console scripts
`sam-mcp`, `sam-mcp-etl`, `sam-mcp-etl-cbip` in `.venv\Scripts\`.

To run anything inside the venv without activating it, prefix with
`uv run` (e.g. `uv run sam-mcp-etl --with-cbip`). Or activate the venv
the classic way:

```powershell
.\.venv\Scripts\Activate.ps1
```

## Build the database

```bash
# All-in-one: SAM XML rebuild + CBIP load (~10–20 min)
uv run sam-mcp-etl --data xml --db db/sam.db --with-cbip

# Or run them separately:
uv run sam-mcp-etl      --data xml          --db db/sam.db
uv run sam-mcp-etl-cbip --sql  exportFr.sql --db db/sam.db
```

Re-run both whenever you receive a new SAM export or a new CBIP dump. The
two datasets are joined at query time via the **CNK** (`dmpp.cnk` ↔
`cbip_mpp.mppcv`) — SAM provides the regulatory facts, CBIP the editorial
commentary. The CBIP step is optional; `get_cbip_notes` will simply return
`None` if the `cbip_*` tables aren't present.

## Run the MCP server

Two transports are supported. **stdio** (default) is for Claude Desktop /
Claude Code on the same machine. **HTTP** (streamable-http) is for LAN
access from other machines.

```powershell
# stdio (Claude Desktop / Claude Code, same machine)
uv run sam-mcp

# HTTP — listens on 0.0.0.0:8000/mcp, reachable from your LAN
uv run sam-mcp --http
```

`SAM_DB` overrides the database path. For HTTP mode, `--host` /
`--port` (or `SAM_HOST` / `SAM_PORT`) override the defaults. To restrict
to localhost only, pass `--host 127.0.0.1`.

On startup the server checks that the database exists: if it's missing it
prints a `FATAL` message and exits non-zero (so a misconfigured `SAM_DB`
fails loudly instead of serving tools that error on every call). Otherwise
it logs the DB path, build timestamp, and row counts for the key tables.
These diagnostics go to **stderr** — in stdio mode stdout carries the
JSON-RPC protocol — so look for them in Claude Desktop's MCP logs:

```text
[sam-mcp] DB db\sam.db (built: 2026-06-04 06:37:42, CBIP edition: 2026-07)
[sam-mcp] row counts: amp=19841, ampp=100191, dmpp=25559, amp_ingredient=27398, substance=14335, atc=7231, cbip_mp=3510, cbip_mpp=8758, cbip_sam=10454
```

In `--http` mode, `GET /status` returns the same information as JSON — useful
for health checks or monitoring:

```bash
curl http://localhost:8000/status
```

```json
{
  "meta": {
    "built_at": "2026-06-04 06:37:42",
    "reference_date": "2026-06-04",
    "cbip_loaded_at": "2026-06-04 06:41:10",
    "cbip_edition": "2607",
    "cbip_export_date": "2026-07"
  },
  "tables": {
    "amp":  {"count": 19841, "description": "Actual Medicinal Product - marketed medicine"},
    "ampp": {"count": 100191, "description": "Actual Medicinal Product Package"},
    ...
  }
}
```

### Viewing the logs — `GET /log`

Also in `--http` mode, `GET /log` shows the server's recent output — the same
stream `docker logs sam-mcp` would give you: startup diagnostics, every tool
call, uvicorn request logs and errors. Handy when the container is reachable
over the LAN or through the reverse proxy but a shell on the host isn't.

Open `http://localhost:8000/log` in a browser for a dark, auto-refreshing
(10s) page; anything else — curl, scripts, monitoring — gets plain text:

```bash
curl http://localhost:8000/log            # plain text
curl "http://localhost:8000/log?lines=50" # last 50 lines only
```

```text
06:37:42 [sam-mcp] DB /data/sam.db (built: 2026-06-04 06:37:42, CBIP edition: 2026-07)
06:37:42 [sam-mcp] row counts: amp=19841, ampp=100191, ...
07:14:03 [sam-mcp] tool=search_medicine args={'query': 'dafalgan'} -> 12 item(s) in 3ms
```

| Query param | Meaning |
| --- | --- |
| `lines=N` | Show only the last N lines. |
| `format=text` / `format=html` | Force the response type instead of guessing from `Accept`. |
| `token=…` | Required when `SAM_LOG_TOKEN` is set (see below). |

| Env var | Default | Meaning |
| --- | --- | --- |
| `SAM_LOG_LINES` | `2000` | Ring-buffer size. |
| `SAM_LOG_TOKEN` | *(unset)* | If set, `/log` requires `?token=…` and returns **403** otherwise. Unset means open, like `/status`. |

The page's tab icon is served from `/favicon.svg`, with `/favicon.ico`
(32×32 + 16×16) as a fallback for browsers that don't take SVG favicons. Both
ship inside the Python package.

The buffer is **in memory only** — it holds the last `SAM_LOG_LINES` lines and
starts empty after every container restart. It is not a replacement for
`docker logs`, just a convenient window onto the same output. Requests to
`/log` itself are excluded from the buffer so the page's auto-refresh doesn't
crowd out real activity. In stdio mode nothing is captured (stdout is the
JSON-RPC channel there and there's no HTTP server to serve the page).

`cbip_export_date` is the month of the CBIP edition (`YYYY-MM`), read from the
dump itself — its PostgreSQL schema name (`SET SEARCH_PATH TO r2607_sql_fr`)
encodes the edition as `r` + YYMM, and `cbip_edition` keeps that raw code
(`2607`), which is also what the download is named (`sql4Emd_Fr_2607A.zip`).
The unzipped file is always `exportFr.sql`, so the file name says nothing
about the data's age. `cbip_loaded_at` is when the dump was loaded, which
tracks the nightly refresh, not the export.

> ⚠️ **LAN exposure** — the server has no authentication. The DB is open
> read-only, so the worst-case is information disclosure (medicine
> data, all of it public anyway). Don't expose it past your trusted LAN
> without a reverse proxy + auth. Windows Firewall will prompt the first
> time you start `--http`; allow access on **Private networks** only.

### Wire it into Claude Desktop / Claude Code

Add to your MCP config (Claude Desktop: `claude_desktop_config.json`,
Claude Code: `~/.claude/settings.json`). Point `command` directly at the
venv's interpreter — Claude Desktop launches the server from an arbitrary
cwd, so we don't go through `uv run`:

Linux / macOS:

```json
{
  "mcpServers": {
    "sam": {
      "command": "/path/to/repo/.venv/bin/python",
      "args": ["-m", "sam_mcp.server"],
      "env": {
        "SAM_DB": "/path/to/repo/db/sam.db"
      }
    }
  }
}
```

Windows (use the `.venv\Scripts\python.exe` interpreter and backslash paths):

```json
{
  "mcpServers": {
    "sam": {
      "command": "C:\\path\\to\\repo\\.venv\\Scripts\\python.exe",
      "args": ["-m", "sam_mcp.server"],
      "env": {
        "SAM_DB": "C:\\path\\to\\repo\\db\\sam.db"
      }
    }
  }
}
```

## Run with Docker

```bash
# Builds the image and serves --http on :8000, mounting db/ read-only
docker compose up --build
```

### Behind a reverse proxy (remote access)

To reach the server from outside your LAN — e.g. published at
`https://sam.example.com/mcp` via Nginx Proxy Manager — run it with:

```bash
sam-mcp --http --behind-proxy [--allowed-hosts sam.example.com]
```

*(In Docker this is the entry point directly. If running locally, prepend `uv run`.)*

- `--behind-proxy` trusts the proxy's `X-Forwarded-*` headers (correct
  client IP / scheme).
- `--allowed-hosts` is an **optional** comma-separated Host allow-list. Omit
  it to accept any Host (the proxy / network is then your only gate).

> **DNS-rebinding protection & HTTP 421.** FastMCP auto-enables a
> localhost-only Host check when it starts. Left as-is, every request
> arriving through a proxy with a public Host header is rejected with
> `421 Invalid Host header`. In `--http` mode this server disables that
> built-in check (the proxy is the trust boundary) and uses `--allowed-hosts`
> instead, so a public hostname works. On the proxy side, forward to the
> container's `:8000` with **Websockets support enabled**.

Both claude.ai and Claude Desktop support remote MCP URLs natively — paste
`https://sam.example.com/mcp` directly in their connector settings. No bridge
required. The server has no authentication; put auth on the reverse proxy if
you need it.

## Automatic database updates

`scripts/refresh-sam.sh` is a production script that runs nightly to keep the
database in sync with the latest SAM and CBIP exports. It works on any Linux
host with Docker and curl/unzip (no Python needed on the host).

The script:

1. **Checks for new SAM exports** — queries the official SAM API for the latest
   version, downloads it if newer than the cached version.
2. **Checks for updated CBIP dumps** — detects the newest French SQL edition
   (released ~monthly), downloads if different from the cached edition.
3. **Rebuilds the database in a throwaway container** — runs `sam-mcp-etl` in a
   temporary Docker container with the new data (10–20 min).
4. **Atomically swaps the database** — stops the running server, replaces the
   old `sam.db` with the new one, clears stale WAL sidecars, and restarts.

### Install on Unraid

1. Place `scripts/refresh-sam.sh` in a persistent location (e.g.
   `/mnt/user/scripts/refresh-sam.sh`). Make it executable: `chmod +x`.
2. Edit the script's **config section** (lines 17–34) to match your setup:
   - `APPDATA` — your app data folder (default: `/mnt/user/appdata/sam-mcp`)
   - `CONTAINER` — your sam-mcp container name
   - `IMAGE` — your sam-mcp image (default: `snarkbe/sam-mcp:latest`)
   - `ENABLE_CBIP` — set to 1 to auto-load CBIP updates, 0 to skip
3. Open the **User Scripts** plugin in Unraid and create a **New Script**.
4. Paste the contents of `refresh-sam.sh`, or set the script path to `/mnt/user/scripts/refresh-sam.sh`.
5. Set the schedule to **Custom** with cron syntax `30 4 * * *` (4:30 AM daily).

### Install on plain Linux

1. Copy `scripts/refresh-sam.sh` to your desired location (e.g. `/opt/sam-mcp/refresh-sam.sh`).
2. Make it executable: `chmod +x /opt/sam-mcp/refresh-sam.sh`
3. Edit the script's config section to match your setup.
4. Add to crontab with `crontab -e`:

   ```shell
   30 4 * * *  /opt/sam-mcp/refresh-sam.sh >> /var/log/refresh-sam.log 2>&1
   ```

## Tools exposed

| Tool | Purpose |
| --- | --- |
| `search_medicine(query, limit)` | Free-text search by brand / prescription name (FR/NL/EN, diacritics-insensitive). |
| `search_everything(query, types, limit)` | **Discoverability router** — fuzzy search across `amp`, `substance`, `nonmedicinal`, `atc`, `impp` and `cbip_mp` in one call, grouped by type with true per-type match counts. Use when you don't know the entity type, a name might be misspelled, or a brand might be Dutch-only — then drill into the returned key with `get_medicine` / `find_by_substance` / `search_nonmedicinal` / `get_atc` / `find_imported` / `get_cbip_notes`. Not a substitute for `search_medicine` when the medicine name is already known. |
| `get_medicine(identifier)` | Full record for a CNK or AMP code: form, route, ingredients, packs. |
| `get_ingredients(identifier)` | Active substances + strengths only. Answers "what is the dose of X?". |
| `find_by_substance(substance, limit)` | Reverse lookup: every AMP containing a molecule. |
| `aggregate_substances(name, min_cnk, max_cnk, atc, limit, offset)` | **Aggregate query** — for every active substance, count distinct CNKs and AMPs. Filter by name (partial), CNK count range, or ATC prefix. One SQL pass over the full catalogue; answers questions like "which substances have exactly one CNK?" or "list all PPIs with their CNK count". |
| `get_substance_cnks(substance_code)` | **Drill-down** — given a substance code from `aggregate_substances`, list every CNK that contains it with pack name, medicine name, and strength. |
| `get_atc(query)` | ATC code/description lookup (exact, prefix, or text). |
| `get_reimbursement(cnk)` | Reimbursement data for a CNK: base/reference prices, flat-rate flag, delivery environment, criteria. |
| `search_nonmedicinal(query, limit)` | Search non-medicinal products (dietary supplements, etc.) by name. |
| `find_compounding(query, limit)` | Find compounding/magistral ingredients by name or synonym. |
| `find_imported(query, limit)` | Search imported medicinal products (IMPP) by name, CNK, or active substance — medicines brought in from abroad for compassionate use or shortages. |
| `get_legal_text(text_key)` | Fetch a reimbursement law text by key (FR/NL content + parent context). |
| `get_cbip_notes(cnk)` | CBIP/BCFI editorial commentary (chapter intro, positioning, notes) for a given CNK. Returns a `coverage` field: `"pack_level"` (direct CBIP pack entry, all price/reimbursement fields populated) or `"product_level"` (re-coded CNK resolved via SAM AMP sibling — product editorial data returned, pack-specific fields null). Returns `None` if outside the CBIP repertoire. |
| `get_database_stats()` | Build metadata + row counts and descriptions for every table — what's searchable and how much of it there is. |

### Aggregate query examples

```text
# All substances with exactly one CNK on the market
aggregate_substances(min_cnk=1, max_cnk=1)

# How many CNKs does metformin have?
aggregate_substances(name="metformin")

# All proton-pump inhibitors with their CNK count  (ATC A02BC)
aggregate_substances(atc="A02BC")

# PPIs sold under a single CNK, then drill into the first result
aggregate_substances(atc="A02BC", min_cnk=1, max_cnk=1)
get_substance_cnks("<substance_code from above>")

# RAAS antihypertensives with more than 5 CNKs
aggregate_substances(atc="C09", min_cnk=6)

# Paginate through all substances (2 000 rows per page)
aggregate_substances(limit=2000, offset=0)
aggregate_substances(limit=2000, offset=2000)
```

> **Note:** the `atc=` filter requires a DB built after the `amp_atc` table was
> introduced (ETL v1.6+). If the table is absent, the tool returns a warning key
> instead of an error.

### Full-text discoverability examples

```text
# One term, every entity type at once — grouped, with a true match count per type
search_everything("paracetamol")

# Restrict to specific types — also matches producer_fr/nl, e.g. "springfield"
search_everything("vitamine d3", types=["nonmedicinal"])

# A CBIP/BCFI-only brand name — resolves to a drillable pack-level CNK
search_everything("dafalgan", types=["cbip_mp"])
get_cbip_notes("<entity_key from above>")

# Restrict to a handful of types instead of searching everything
search_everything("omeprazole", types=["amp", "substance", "atc"])
```

`counts_by_type` reports the true number of matches per type even when the
per-type result list is truncated by `limit` — e.g. you can tell "9 AMPs, 1
substance, 214 nonmedicinal products" apart without 214 supplements drowning
out the 9 medicines that actually mattered. `cbip_mp` is only searched if the
DB was built with `--with-cbip`.

## Schema (high level)

```text
-- Reference
substance(code PK, name_fr/nl/en, type)
atc(code PK, description)
pharma_form(code PK, name_*)
route(code PK, name_*)
vtm(code PK, name_fr/nl)                          -- Virtual Therapeutic Molecules

-- Medicines
amp(code PK, name_*, status, medicine_type, company, ...)
amp_component(amp_code, seq) -> form + route
amp_ingredient(amp_code, component_seq, rank) -> substance + strength
ampp(cti_extended PK, amp_code, pack info, price)
dmpp(cnk PK, cti_extended, amp_code)
amp_atc(amp_code, atc_code PK)                    -- AMP → ATC link (from AMP file Ampp/Atc)
amp_fts, substance_fts, nonmedicinal_fts,
atc_fts, impp_fts                                 -- FTS5 indexes (+ cbip_mp_fts, optional CBIP)

-- Imported medicines (IMPP)
impp(id PK, cnk, name, country, strength, pack_size, pharma_form_*)
impp_substance(impp_id, substance_code PK) -> name_fr/nl
impp_route(impp_id, route_code PK) -> route_fr/nl

-- Reimbursement
reimbursement(cnk, delivery_environment, valid_from PK, prices, flags)
reimbursement_criterion(cnk, delivery_environment, valid_from, category, code PK)

-- Non-medicinal & compounding
nonmedicinal(code PK, name_fr/nl, category, commercial_status, producer/distributor)
compounding_ingredient(code PK, product_id)
compounding_synonym(code, lang, rank PK, name)

-- Reimbursement law
legal_basis(key PK, title_fr/nl, type, effective_on)
legal_reference(basis_key, ref_key PK, parent_ref_key, title_fr/nl, type)
legal_text(basis_key, text_key PK, ref_key, content_fr/nl, type, sequence_nr)
```

The ETL picks the **currently valid** `<Data>` slice per entity (today
between `from`/`to`); historical slices are not stored. The reference date
is overridable via `--today YYYY-MM-DD` if you want a frozen snapshot.
