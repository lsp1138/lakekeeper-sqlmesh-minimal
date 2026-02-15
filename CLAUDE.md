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
