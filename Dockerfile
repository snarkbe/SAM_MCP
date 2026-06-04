FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ src/

RUN uv sync --frozen --no-dev

ENV SAM_DB=/data/sam.db
EXPOSE 8000

CMD ["uv", "run", "sam-mcp", "--http"]
