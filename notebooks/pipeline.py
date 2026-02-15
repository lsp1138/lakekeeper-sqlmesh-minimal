import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # Bronze / Silver / Gold Pipeline

        This notebook walks through the multi-step SQLMesh pipeline that
        transforms raw order data through three layers, all stored as Iceberg
        tables in the Lakekeeper catalog.

        **Prerequisites**: `docker compose up -d` and `cd transform && uv run sqlmesh plan --auto-apply`
        """
    )
    return (mo,)


@app.cell
def _(mo):
    import duckdb

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg")
    conn.execute("LOAD iceberg")
    conn.execute("""
        CREATE SECRET lakekeeper_secret (
            TYPE ICEBERG, TOKEN 'dummy_token'
        )
    """)
    conn.execute("""
        CREATE SECRET minio_secret (
            TYPE S3, KEY_ID 'minio-root-user', SECRET 'minio-root-password',
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'local-01'
        )
    """)
    conn.execute("""
        ATTACH 'warehouse' AS warehouse (
            TYPE ICEBERG, ENDPOINT 'http://localhost:8181/catalog',
            SECRET lakekeeper_secret, ACCESS_DELEGATION_MODE 'none'
        )
    """)

    mo.md("Connected to Lakekeeper.")
    return (conn,)


@app.cell
def _(conn, mo):
    bronze = conn.execute(
        "SELECT * FROM warehouse.demo.bronze_orders ORDER BY order_id"
    ).fetchdf()

    mo.md(
        f"""
        ## Bronze Layer — Raw Ingested Data

        All 18 rows from the seed CSV, including all statuses (completed, returned, pending).
        No transformations applied.

        {mo.as_html(bronze)}
        """
    )
    return


@app.cell
def _(conn, mo):
    silver = conn.execute(
        "SELECT * FROM warehouse.demo.silver_orders ORDER BY order_id"
    ).fetchdf()

    mo.md(
        f"""
        ## Silver Layer — Cleaned & Deduplicated

        - Filtered to `status = 'completed'` only (removed returned and pending)
        - Deduplicated on all columns
        - Added computed `total_price` column (`quantity * unit_price`)
        - Cast `order_date` from string to DATE

        **{len(silver)} rows** (down from 18 in bronze)

        {mo.as_html(silver)}
        """
    )
    return


@app.cell
def _(conn, mo):
    gold = conn.execute(
        "SELECT * FROM warehouse.demo.gold_daily_revenue ORDER BY order_date"
    ).fetchdf()

    mo.md(
        f"""
        ## Gold Layer — Business-Ready Aggregates

        Daily revenue summary: order count, unique customers, total revenue,
        and average order value.

        {mo.as_html(gold)}
        """
    )
    return


@app.cell
def _(mo):
    import json
    import urllib.request

    config_resp = urllib.request.urlopen(
        "http://localhost:8181/catalog/v1/config?warehouse=warehouse"
    )
    config = json.loads(config_resp.read())
    prefix = config["defaults"]["prefix"]

    tables_resp = urllib.request.urlopen(
        f"http://localhost:8181/catalog/v1/{prefix}/namespaces/demo/tables"
    )
    tables = json.loads(tables_resp.read())
    table_names = [t["name"] for t in tables["identifiers"]]

    mo.md(
        f"""
        ## Catalog Verification

        All tables registered in the Lakekeeper Iceberg catalog under the `demo` namespace:

        {", ".join(f"`{t}`" for t in sorted(table_names))}
        """
    )
    return


if __name__ == "__main__":
    app.run()
