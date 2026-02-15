import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # Bronze / Silver / Gold Pipeline

        This notebook runs the SQLMesh pipeline end-to-end, transforming raw
        order data through three layers — all stored as Iceberg tables in
        the Lakekeeper catalog.

        **Prerequisites**: `docker compose up -d` must be running.
        """
    )
    return (mo,)


@app.cell
def _(mo):
    from pathlib import Path

    from sqlmesh.core import Context

    # Point SQLMesh at the transform/ project directory
    transform_dir = Path(__file__).parent.parent / "transform"
    ctx = Context(paths=str(transform_dir), gateway="local")

    model_names = list(ctx.models.keys())
    mo.md(
        f"**SQLMesh context loaded** with {len(model_names)} models:\n\n"
        + "\n".join(f"- `{m}`" for m in sorted(model_names))
    )
    return (ctx, mo, transform_dir)


@app.cell
def _(ctx, mo):
    # Create and apply the plan (no interactive prompts)
    plan = ctx.plan(environment="prod", no_prompts=True, auto_apply=True)

    mo.md("**SQLMesh plan applied.** All models are up to date.")
    return


@app.cell
def _(ctx, mo):
    bronze = ctx.fetchdf("SELECT * FROM warehouse.demo.bronze_orders ORDER BY order_id")

    mo.md(
        f"""
        ## Bronze Layer — Raw Ingested Data

        All {len(bronze)} rows from the seed CSV, including all statuses
        (completed, returned, pending). No transformations applied.

        {mo.as_html(bronze)}
        """
    )
    return


@app.cell
def _(ctx, mo):
    silver = ctx.fetchdf("SELECT * FROM warehouse.demo.silver_orders ORDER BY order_id")

    mo.md(
        f"""
        ## Silver Layer — Cleaned & Deduplicated

        - Filtered to `status = 'completed'` only
        - Deduplicated on all columns
        - Added computed `total_price` column (`quantity * unit_price`)
        - Cast `order_date` from string to DATE

        **{len(silver)} rows** (down from bronze)

        {mo.as_html(silver)}
        """
    )
    return


@app.cell
def _(ctx, mo):
    gold = ctx.fetchdf(
        "SELECT * FROM warehouse.demo.gold_daily_revenue ORDER BY order_date"
    )

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

        All tables registered in the Lakekeeper Iceberg catalog under `demo`:

        {", ".join(f"`{t}`" for t in sorted(table_names))}
        """
    )
    return


if __name__ == "__main__":
    app.run()
