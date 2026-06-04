# SAM MCP

Local MCP server that answers questions about Belgian medicines from the
official SAM v2 XML exports (FAGG/AFMPS, eHealth), plus CBIP/BCFI editorial
commentary.

Examples it can answer:
- "What is the dose of *Dafalgan 500*?"
- "Which molecule does *Symbicort* contain?"
- "Which medicines contain *salbutamol*?"
- "What's the CNK 3104965?"

## Layout

| Path | What's there |
|---|---|
| `src/sam_mcp/` | Python package — ETL + MCP server. |
| `db/sam.db` | SQLite database produced by the ETL (gitignored). |
| `xml/` | SAM v2 XML exports (gitignored — drop the official files here). |
| `exportFr.sql` | CBIP/BCFI repertoire dump (gitignored). |

Source data lives outside git because it's large and regenerable. Get the
XML from <https://www.vas.ehealth.fgov.be/websamcivics/samcivics/> and the
CBIP dump from <https://www.cbip.be/fr/download>.

## How it works

1. **ETL** — `sam_mcp.etl` streams the XML files (`AMP`, `REF`) into a
   single SQLite file (`sam.db`) using `lxml.iterparse`. The 1.5 GB AMP file
   is processed without ever loading it fully into memory.
2. **Server** — `sam_mcp.server` is a stdio MCP server (FastMCP) that
   exposes read-only query tools over `sam.db`.

Reimbursement (RMB/RML) and Chapter IV are intentionally deferred for now;
they can be added later without schema changes.

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

```
[sam-mcp] DB db\sam.db (built: 2026-06-04 06:37:42)
[sam-mcp] row counts: amp=19841, ampp=100191, dmpp=25559, amp_ingredient=27398, substance=14335, atc=7231, cbip_mp=3510, cbip_mpp=8758, cbip_sam=10454
```

> ⚠️ **LAN exposure** — the server has no authentication. The DB is open
> read-only, so the worst-case is information disclosure (medicine
> data, all of it public anyway). Don't expose it past your trusted LAN
> without a reverse proxy + auth. Windows Firewall will prompt the first
> time you start `--http`; allow access on **Private networks** only.

### Wire it into Claude Desktop / Claude Code

Add to your MCP config (Claude Desktop: `claude_desktop_config.json`,
Claude Code: `claude_code_config.json` or `settings.json`). Point
`command` directly at the venv's interpreter — Claude Desktop launches the
server from an arbitrary cwd, so we don't go through `uv run`:

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

## Run with Docker

```bash
# Builds the image and serves --http on :8000, mounting db/ read-only
docker compose up --build
```

### Behind a reverse proxy (remote access)

To reach the server from outside your LAN — e.g. published at
`https://sam.example.com/mcp` via Nginx Proxy Manager — run it with:

```
sam-mcp --http --behind-proxy [--allowed-hosts sam.example.com]
```

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

Claude Desktop's config file only speaks stdio, so to use a remote URL point
it at the [`mcp-remote`](https://www.npmjs.com/package/mcp-remote) bridge
(requires Node.js):

```json
{
  "mcpServers": {
    "sam": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://sam.example.com/mcp"]
    }
  }
}
```

The server has no authentication, so `mcp-remote`'s OAuth discovery is a
no-op — put auth on the reverse proxy if you need it.

## Tools exposed

| Tool | Purpose |
|---|---|
| `search_medicine(query, limit)` | Free-text search by brand / prescription name (FR/NL/EN, diacritics-insensitive). |
| `get_medicine(identifier)` | Full record for a CNK or AMP code: form, route, ingredients, packs. |
| `get_ingredients(identifier)` | Active substances + strengths only. Answers "what is the dose of X?". |
| `find_by_substance(substance, limit)` | Reverse lookup: every AMP containing a molecule. |
| `get_atc(query)` | ATC code/description lookup (exact, prefix, or text). |
| `get_cbip_notes(cnk)` | CBIP/BCFI editorial commentary (chapter intro, positioning, notes) for a given CNK. Returns `None` if outside the CBIP repertoire. |
| `db_info()` | Build metadata + row counts. |

## Schema (high level)

```
substance(code PK, name_fr/nl/en, type)
atc(code PK, description)
pharma_form(code PK, name_*)
route(code PK, name_*)
amp(code PK, name_*, status, medicine_type, company, ...)
amp_component(amp_code, seq) -> form + route
amp_ingredient(amp_code, component_seq, rank) -> substance + strength
ampp(cti_extended PK, amp_code, pack info, price)
dmpp(cnk PK, cti_extended, amp_code)
amp_fts, substance_fts          -- FTS5 indexes
```

The ETL picks the **currently valid** `<Data>` slice per entity (today
between `from`/`to`); historical slices are not stored. The reference date
is overridable via `--today YYYY-MM-DD` if you want a frozen snapshot.
