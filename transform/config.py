import typing as t

from sqlmesh.core import engine_adapter
from sqlmesh.core.config import (
    Config,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.config.connection import DuckDBConnectionConfig
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter


class IcebergDuckDBEngineAdapter(DuckDBEngineAdapter):
    """DuckDB adapter patched for Iceberg catalog limitations.

    Iceberg via DuckDB doesn't support:
    - CREATE OR REPLACE TABLE
    - CREATE VIEW
    - USE <catalog> (without schema qualifier)
    """

    SUPPORTS_REPLACE_TABLE = False

    def set_current_catalog(self, catalog: str) -> None:
        # DuckDB can't USE an Iceberg catalog without a schema qualifier — skip.
        pass

    def create_view(
        self,
        view_name: t.Any,
        query_or_df: t.Any,
        replace: bool = True,
        **kwargs: t.Any,
    ) -> None:
        # Iceberg doesn't support views. Create a table copy instead.
        # First drop the existing object if replacing.
        if replace:
            self.drop_view(view_name, ignore_if_not_exists=True)
            self.drop_table(view_name, exists=True)
        self.replace_query(
            view_name,
            query_or_df,
            supports_replace_table_override=False,
            **kwargs,
        )

    def drop_view(
        self,
        view_name: t.Any,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        cascade: bool = False,
    ) -> None:
        # Views don't exist in Iceberg, so dropping is a no-op.
        pass


class IcebergDuckDBConnectionConfig(DuckDBConnectionConfig):
    """DuckDB connection that attaches an Iceberg REST catalog."""

    iceberg_endpoint: str = "http://localhost:8181/catalog"
    iceberg_warehouse: str = "warehouse"

    @property
    def _engine_adapter(self) -> t.Type[engine_adapter.EngineAdapter]:
        return IcebergDuckDBEngineAdapter

    @property
    def _cursor_init(self) -> t.Optional[t.Callable[[t.Any], None]]:
        base_init = super()._cursor_init
        endpoint = self.iceberg_endpoint
        warehouse = self.iceberg_warehouse

        def init(cursor: t.Any) -> None:
            if base_init:
                base_init(cursor)
            # Attach the Iceberg REST catalog (secrets already created by base init)
            cursor.execute(
                f"ATTACH IF NOT EXISTS '{warehouse}' AS {warehouse} ("
                f"  TYPE ICEBERG,"
                f"  ENDPOINT '{endpoint}',"
                f"  SECRET lakekeeper_secret,"
                f"  ACCESS_DELEGATION_MODE 'none'"
                f")"
            )
            # Set the Iceberg catalog + schema as default
            cursor.execute(f"USE {warehouse}.demo")

        return init


config = Config(
    gateways={
        "local": GatewayConfig(
            connection=IcebergDuckDBConnectionConfig(
                extensions=["iceberg"],
                secrets={
                    "lakekeeper_secret": {
                        "type": "iceberg",
                        "token": "dummy_token",
                    },
                    "minio_secret": {
                        "type": "s3",
                        "key_id": "minio-root-user",
                        "secret": "minio-root-password",
                        "endpoint": "localhost:9000",
                        "url_style": "path",
                        "use_ssl": "false",
                        "region": "local-01",
                    },
                },
                iceberg_endpoint="http://localhost:8181/catalog",
                iceberg_warehouse="warehouse",
            ),
            state_connection=DuckDBConnectionConfig(
                database="state.db",
            ),
        ),
    },
    default_gateway="local",
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb",
        start="2024-01-01",
    ),
)
