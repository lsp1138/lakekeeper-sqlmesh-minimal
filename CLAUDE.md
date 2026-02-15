# CLAUDE.md

## Project Overview

Minimal local lakehouse stack: Lakekeeper + MinIO + DuckDB + SQLMesh.

This is a research/demo repo showing the simplest possible setup for a
Fabric-like experience on a single node with a shared Iceberg catalog.
No vendor lock-in, no Spark, no Kubernetes, no cloud services.

## Stack

- **Lakekeeper**: Apache Iceberg REST Catalog (Rust, port 8181)
- **MinIO**: S3-compatible object storage (ports 9000/9001)
- **Postgres**: Lakekeeper metadata backend
- **DuckDB**: Embedded SQL engine, connects to Lakekeeper via Iceberg REST
- **SQLMesh**: Transformation/modeling layer on top of DuckDB + Iceberg

## Key Details

- Lakekeeper runs in unsecured mode (no auth) for local dev
- DuckDB uses iceberg extension with dummy token for unsecured catalogs
- DuckDB 1.4+ supports Iceberg reads AND writes via REST catalog
- Warehouse config points MinIO as S3 backend with path-style access
- SQLMesh uses DuckDB as its engine, Iceberg catalog for table management

## Known Workarounds

### S3 endpoint: Docker vs host

Lakekeeper's warehouse config stores the S3 endpoint as `http://minio:9000`
(Docker-internal hostname). Lakekeeper validates this on warehouse creation by
writing a test file, so it must be reachable from inside Docker.

DuckDB runs on the host and can't resolve `minio:9000`. By default, DuckDB
uses "vended credentials" from the catalog which include this unreachable
endpoint. The fix:

1. Create a separate S3 secret in DuckDB pointing to `localhost:9000`
2. Use `ACCESS_DELEGATION_MODE 'none'` when ATTACHing, which tells DuckDB
   to use its own S3 secret instead of the catalog's vended credentials

See `notebooks/getting_started.py` and `transform/config.py` for examples.

### DuckDB-Iceberg limitations (affects SQLMesh)

DuckDB's Iceberg extension does not support:
- `CREATE OR REPLACE TABLE` — use DROP + CREATE instead
- `CREATE VIEW` — not implemented for Iceberg catalogs
- `USE <catalog>` — must include schema: `USE warehouse.demo`

SQLMesh relies on all three. The custom `IcebergDuckDBEngineAdapter` in
`transform/config.py` works around these:
- `SUPPORTS_REPLACE_TABLE = False` — forces DROP + CREATE path
- `create_view()` overridden to create a table copy instead
- `set_current_catalog()` is a no-op (relies on fully qualified names)

## Development

- Follow issues.yaml as the canonical task list
- Work on one issue at a time
- Prefer the simplest working solution
- No over-engineering

## Useful References

- Lakekeeper docs: https://docs.lakekeeper.io
- Lakekeeper minimal example: https://github.com/lakekeeper/lakekeeper/tree/main/examples/minimal
- DuckDB Iceberg REST catalogs: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
- SQLMesh DuckDB integration: https://sqlmesh.readthedocs.io/en/latest/integrations/engines/duckdb/
