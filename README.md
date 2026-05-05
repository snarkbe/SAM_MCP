# SAM

Local tooling around the Belgian medicines database (SAM v2 from FAGG/AFMPS,
eHealth) plus CBIP/BCFI editorial commentary.

## Layout

| Path | What's there |
|---|---|
| [sam_mcp/](sam_mcp/) | Python package — ETL + MCP server. See [sam_mcp/README.md](sam_mcp/README.md). |
| `xml/` | SAM v2 XML exports (gitignored — drop the official files here). |
| `exportFr.sql` | CBIP/BCFI repertoire dump (gitignored). |
| `db/sam.db` | SQLite database produced by the ETL (gitignored). |

Source data lives outside git because it's large and regenerable. Get the
XML from <https://www.vas.ehealth.fgov.be/websamcivics/samcivics/> and the
CBIP dump from <https://www.cbip.be/fr/download>.

## Quick start

```powershell
cd d:\Git\SAM\sam_mcp
uv sync
uv run sam-mcp-etl --data d:\Git\SAM\xml --db d:\Git\SAM\db\sam.db --with-cbip
uv run sam-mcp
```

Full setup, MCP wiring, and tool reference: [sam_mcp/README.md](sam_mcp/README.md).
