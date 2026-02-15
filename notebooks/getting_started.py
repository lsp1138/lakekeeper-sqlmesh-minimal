import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # Getting Started with the Local Lakehouse

        This notebook demonstrates the full loop:
        DuckDB → Lakekeeper (Iceberg REST Catalog) → MinIO (S3 storage).

        **Prerequisites**: `docker compose up -d` must be running.
        """
    )
    return (mo,)


@app.cell
def _(mo):
    import duckdb

    conn = duckdb.connect()

    # Install and load the iceberg extension
    conn.execute("INSTALL iceberg")
    conn.execute("LOAD iceberg")

    # Create a secret for the unsecured Lakekeeper catalog
    conn.execute("""
        CREATE SECRET lakekeeper_secret (
            TYPE ICEBERG,
            TOKEN 'dummy_token'
        )
    """)

    # S3 secret so DuckDB can reach MinIO on localhost
    # (the catalog returns minio:9000 which is only reachable inside Docker)
    conn.execute("""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID 'minio-root-user',
            SECRET 'minio-root-password',
            ENDPOINT 'localhost:9000',
            URL_STYLE 'path',
            USE_SSL false,
            REGION 'local-01'
        )
    """)

    # Attach the warehouse via Iceberg REST
    # ACCESS_DELEGATION_MODE 'none' tells DuckDB to use its own S3 secret
    # instead of vended credentials from the catalog
    conn.execute("""
        ATTACH 'warehouse' AS warehouse (
            TYPE ICEBERG,
            ENDPOINT 'http://localhost:8181/catalog',
            SECRET lakekeeper_secret,
            ACCESS_DELEGATION_MODE 'none'
        )
    """)

    mo.md("**Connected to Lakekeeper** via Iceberg REST catalog.")
    return (conn,)


@app.cell
def _(conn, mo):
    # Create a namespace (Iceberg namespace = DuckDB schema)
    conn.execute("CREATE SCHEMA IF NOT EXISTS warehouse.demo")

    # Create a table through the Iceberg catalog
    conn.execute("DROP TABLE IF EXISTS warehouse.demo.cities")
    conn.execute("""
        CREATE TABLE warehouse.demo.cities (
            name VARCHAR,
            country VARCHAR,
            population INTEGER
        )
    """)

    mo.md("**Created** namespace `demo` and table `cities` in the Iceberg catalog.")
    return


@app.cell
def _(conn, mo):
    conn.execute("""
        INSERT INTO warehouse.demo.cities VALUES
            ('Amsterdam', 'Netherlands', 921402),
            ('Berlin', 'Germany', 3748148),
            ('Copenhagen', 'Denmark', 794128),
            ('Dublin', 'Ireland', 592713),
            ('Edinburgh', 'UK', 524930)
    """)

    mo.md("**Inserted** 5 rows into `warehouse.demo.cities`.")
    return


@app.cell
def _(conn):
    # Query the data back through the Iceberg catalog
    conn.execute(
        "SELECT * FROM warehouse.demo.cities ORDER BY population DESC"
    ).fetchdf()
    return


@app.cell
def _(mo):
    import json
    import urllib.request

    # Query Lakekeeper management API to show the warehouse
    resp = urllib.request.urlopen("http://localhost:8181/management/v1/warehouse")
    warehouses = json.loads(resp.read())

    mo.md(
        f"**Warehouse metadata** from Lakekeeper REST API:\n\n```json\n{json.dumps(warehouses, indent=2)}\n```"
    )
    return (json, urllib)


@app.cell
def _(json, mo, urllib):
    # List namespaces in the catalog
    # The prefix is the warehouse UUID returned by the config endpoint
    config_resp = urllib.request.urlopen(
        "http://localhost:8181/catalog/v1/config?warehouse=warehouse"
    )
    config = json.loads(config_resp.read())
    prefix = config["defaults"]["prefix"]

    ns_resp = urllib.request.urlopen(
        f"http://localhost:8181/catalog/v1/{prefix}/namespaces"
    )
    namespaces = json.loads(ns_resp.read())

    tables_resp = urllib.request.urlopen(
        f"http://localhost:8181/catalog/v1/{prefix}/namespaces/demo/tables"
    )
    tables = json.loads(tables_resp.read())

    mo.md(f"**Namespaces**: {namespaces}\n\n**Tables in `demo`**: {tables}")
    return


if __name__ == "__main__":
    app.run()
