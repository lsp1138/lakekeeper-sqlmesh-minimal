# lakekeeper-sqlmesh-minimal

Minimal local lakehouse stack: **Lakekeeper + MinIO + DuckDB + SQLMesh**.

A Fabric/Databricks-like experience on a single node with a shared Iceberg catalog.
No vendor lock-in, no Spark, no Kubernetes, no cloud services - all open source and portable.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────┐
│   DuckDB    │────▶│   Lakekeeper     │────▶│  MinIO  │
│  (queries)  │     │ (Iceberg REST    │     │  (S3)   │
│             │     │  Catalog :8181)  │     │  :9000  │
└─────────────┘     └──────────────────┘     └─────────┘
       ▲                     ▲
       │                     │
┌──────┴──────┐     ┌────────┴───────┐
│   SQLMesh   │     │   Postgres     │
│ (transforms)│     │  (metadata)    │
└─────────────┘     └────────────────┘
```

- **Lakekeeper** — Rust-based Apache Iceberg REST Catalog
- **MinIO** — S3-compatible object storage for Iceberg data files
- **Postgres** — Lakekeeper's metadata backend
- **DuckDB** — Embedded SQL engine with Iceberg read/write support
- **SQLMesh** — Transformation layer (bronze/silver/gold pipeline)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [uv](https://docs.astral.sh/uv/) (Python package manager)

## Quick Start

```bash
# 1. Start the stack (Lakekeeper, MinIO, Postgres + bootstrap)
docker compose up -d

# 2. Install Python dependencies
uv sync

# 3. Explore with the getting-started notebook
uv run marimo edit notebooks/getting_started.py

# 4. Run the SQLMesh pipeline
cd transform
uv run sqlmesh plan --auto-apply

# 5. Explore the pipeline results
cd ..
uv run marimo edit notebooks/pipeline.py

# Or run SQLMesh from within a notebook (no CLI needed)
uv run marimo edit notebooks/sqlmesh_pipeline.py
```

## What Happens on `docker compose up`

Seven services start in dependency order:

1. **Postgres** and **MinIO** start, wait until healthy
2. **createbuckets** — creates the `warehouse` bucket in MinIO
3. **migrate** — runs Lakekeeper's database schema migration
4. **lakekeeper** — starts the Iceberg REST Catalog server
5. **bootstrap** — initializes Lakekeeper (`POST /management/v1/bootstrap`)
6. **initialwarehouse** — registers the warehouse pointing to MinIO

After ~15 seconds, the stack is ready. No manual steps.

## Project Structure

```
.
├── docker-compose.yml          # 7 services on a single bridge network
├── create-warehouse.json       # Warehouse config (MinIO S3 profile)
├── pyproject.toml              # Python deps: duckdb, marimo, pandas, sqlmesh
├── notebooks/
│   ├── getting_started.py      # Marimo: DuckDB → Lakekeeper → MinIO basics
│   ├── pipeline.py             # Marimo: bronze/silver/gold results viewer
│   └── sqlmesh_pipeline.py     # Marimo: runs SQLMesh plan/apply end-to-end
└── transform/                  # SQLMesh project
    ├── config.py               # DuckDB + Iceberg catalog config
    ├── models/
    │   ├── raw_cities.sql      # Seed: city data
    │   ├── city_stats.sql      # Full: continent aggregates
    │   ├── bronze_orders.sql   # Seed: raw order data
    │   ├── silver_orders.sql   # Full: cleaned, completed only
    │   └── gold_daily_revenue.sql  # Full: daily revenue summary
    └── seeds/
        ├── raw_cities.csv
        └── raw_orders.csv
```

## Known Workarounds

See [CLAUDE.md](CLAUDE.md#known-workarounds) for details on:

- **S3 endpoint mismatch** — Lakekeeper uses `minio:9000` (Docker-internal), DuckDB on the host uses `localhost:9000` via `ACCESS_DELEGATION_MODE 'none'`
- **DuckDB-Iceberg limitations** — No `CREATE OR REPLACE`, no views, no `USE <catalog>`. Custom adapter in `transform/config.py` handles all three.

## Links

- [Lakekeeper docs](https://docs.lakekeeper.io)
- [DuckDB Iceberg REST catalogs](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs)
- [SQLMesh DuckDB integration](https://sqlmesh.readthedocs.io/en/latest/integrations/engines/duckdb/)
- [Marimo notebooks](https://docs.marimo.io)

## Teardown

```bash
docker compose down -v   # stops containers and removes volumes
```
