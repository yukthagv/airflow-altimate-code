# Airflow 3 Migration Notes:
#   - BaseOperator import updated: airflow.models.baseoperator -> airflow.sdk
#     airflow.sdk is the new stable public interface in Airflow 3 (AIP-72).
#     The old path (airflow.models.baseoperator) still works in Airflow 3.0 via
#     a compatibility shim, but airflow.sdk is the preferred path going forward.
#   - context["ts"] is still available in Airflow 3 task context (no change).
#   - self.log uses structlog under the hood in Airflow 3 but the API is unchanged.
#   - DuckDBHook constructor call unchanged; DuckDBHook.get_hook() is also valid.

try:
    # Airflow 3: preferred stable public interface
    from airflow.sdk import BaseOperator
except ImportError:
    # Airflow 2 fallback (keeps backward compatibility during transition)
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]

from duckdb_provider.hooks.duckdb_hook import DuckDBHook


class ExcelToDuckDBOperator(BaseOperator):
    """
    Operator that loads an Excel file into a new table in a DuckDB database.

    Uses the DuckDB spatial extension (st_read) to read Excel files.

    :param table_name: Name of the table to create in DuckDB.
    :param excel_path: Path to the Excel file to load.
    :param sheet_name: Name of the sheet in the Excel file to load.
    :param duckdb_conn_id: Airflow connection ID for DuckDB.
    """

    def __init__(
        self,
        table_name: str,
        excel_path: str,
        sheet_name: str = "Sheet1",
        duckdb_conn_id: str = "duckdb_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.excel_path = excel_path
        self.sheet_name = sheet_name
        self.duckdb_conn_id = duckdb_conn_id

    def execute(self, context):
        ts = context["ts"]
        duckdb_hook = DuckDBHook(duckdb_conn_id=self.duckdb_conn_id)
        duckdb_conn = duckdb_hook.get_conn()
        duckdb_conn.execute("install spatial;")
        duckdb_conn.execute("load spatial;")
        duckdb_conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_name} AS
            SELECT * FROM st_read('{self.excel_path}', layer='{self.sheet_name}');"""
        )
        self.log.info(f"{ts}: created table {self.table_name} from {self.excel_path}!")
        return self.table_name, self.excel_path
