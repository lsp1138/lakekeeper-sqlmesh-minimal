import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md(
        """
        # SQLMesh Pipeline

        Run the bronze/silver/gold SQLMesh pipeline from Python — no CLI needed.
        Models are applied as Iceberg tables through the Lakekeeper catalog.

        **Prerequisites**: `docker compose up -d` must be running.
        """
    )
    return (mo,)


@app.cell
def _(mo):
    from pathlib import Path

    from sqlmesh import Context

    transform_dir = Path(__file__).parent.parent / "transform"
    ctx = Context(paths=str(transform_dir), gateway="local")

    model_names = sorted(ctx.models.keys())
    mo.md(
        f"**Loaded {len(model_names)} models** from `transform/`:\n\n"
        + "\n".join(f"- `{m}`" for m in model_names)
    )
    return (ctx, transform_dir)


@app.cell
def _(ctx, mo):
    plan = ctx.plan(environment="prod", no_prompts=True, auto_apply=True)

    mo.md("**Plan applied.** All models are up to date.")
    return


@app.cell
def _(ctx, mo):
    bronze = ctx.fetchdf("SELECT * FROM warehouse.demo.bronze_orders ORDER BY order_id")

    mo.md(
        f"""## Bronze Layer

Raw seed data — {len(bronze)} rows, no transforms.

{mo.as_html(bronze)}"""
    )
    return


@app.cell
def _(ctx, mo):
    silver = ctx.fetchdf("SELECT * FROM warehouse.demo.silver_orders ORDER BY order_id")

    mo.md(
        f"""## Silver Layer

Filtered to completed orders, deduplicated, added `total_price`.

**{len(silver)} rows**

{mo.as_html(silver)}"""
    )
    return


@app.cell
def _(ctx, mo):
    gold = ctx.fetchdf(
        "SELECT * FROM warehouse.demo.gold_daily_revenue ORDER BY order_date"
    )

    mo.md(
        f"""## Gold Layer

Daily revenue aggregates.

{mo.as_html(gold)}"""
    )
    return


if __name__ == "__main__":
    app.run()
