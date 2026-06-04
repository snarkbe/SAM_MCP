# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies and create venv
uv sync

# Build the database (SAM XML + CBIP, ~10–20 min)
uv run sam-mcp-etl --data xml --db db/sam.db --with-cbip

# Build SAM only (faster, CBIP optional)
uv run sam-mcp-etl --data xml --db db/sam.db

# Load CBIP into an existing DB
uv run sam-mcp-etl-cbip --sql exportFr.sql --db db/sam.db

# Run the MCP server (stdio — for Claude Desktop / Claude Code)
uv run sam-mcp

# Run the MCP server (HTTP — LAN access on :8000/mcp)
uv run sam-mcp --http
```

There are no tests in this repository.

## Architecture

The project is a single Python package (`src/sam_mcp/`) with three entry points:

**`sam_mcp.etl`** — streams the SAM v2 XML exports into SQLite using `lxml.iterparse`. The 1.5 GB `AMP` file is processed element-by-element; each `<Amp>` element is cleared after processing to keep memory flat. For every entity only the currently-valid `<Data>` slice (today between `from`/`to`) is stored — historical slices are discarded. Picks the newest `PREFIX-*.xml` file in the data dir automatically.

**`sam_mcp.etl_cbip`** — loads the CBIP PostgreSQL dump (`exportFr.sql`) into the same SQLite DB under `cbip_*` table names. Translates PG-specific types to SQLite equivalents via regex on `CREATE TABLE` statements only. Returns exit code `0` (clean), `2` (row-level errors — non-fatal, means a few INSERT rows were unparseable), or `1` (fatal). Code `2` is expected in production and should not be treated as failure.

**`sam_mcp.server`** — FastMCP server. All tools open the DB read-only (`file:...?mode=ro`). Diagnostics (startup counts, errors) go to **stderr** — stdout is the JSON-RPC channel in stdio mode. In `--http` mode, FastMCP's built-in DNS-rebinding protection is explicitly disabled (the proxy/LAN is the trust boundary); `--allowed-hosts` is the opt-in replacement.

**`schema.sql`** — applied at ETL start (line 1: `PRAGMA journal_mode = WAL`). The DB is a WAL database. When swapping a live DB, you must stop the server, rename the file, then remove the stale `-wal`/`-shm` sidecars before restarting — a bare rename leaves orphaned sidecars next to the new file.

**Key join**: SAM and CBIP are joined at query time via CNK: `dmpp.cnk` ↔ `cbip_mpp.mppcv` (column named `inncnk` or `mppcv` depending on direction). The `get_cbip_notes` tool checks for table existence via `sqlite_master` so the CBIP load is optional.

**DB path**: resolved from `SAM_DB` env var, default `db/sam.db`. In Docker, `SAM_DB=/data/sam.db` with `/data` bind-mounted from the host.

## Production deployment

Runs on Unraid as a single Docker container (`snarkbe/sam-mcp`), **not** docker-compose. Nightly DB refresh is scheduled via the User Scripts plugin at `30 4 * * *` using `scripts/refresh-sam.sh`. The script downloads SAM and CBIP, runs the ETL inside a throwaway `docker run --rm` container of the same image (the Unraid host has no Python/uv), then does the stop→swap→clear-WAL→start sequence.
