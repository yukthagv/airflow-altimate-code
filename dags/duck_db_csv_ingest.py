"""
### Ingest sample sales CSV into DuckDB and aggregate totals by region

This DAG creates the shared DuckDB pool used in the project, loads the
`include/sample_sales.csv` file into a DuckDB table called `sales`, and logs
total sales amount by region. The pool serializes the write task so only one
writer updates DuckDB at a time.
"""

from logging import getLogger
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from pendulum import datetime

CSV_PATH = Path("include/sample_sales.csv")
LOCAL_DUCKDB_CONN_ID = "my_local_duckdb_conn"
DUCKDB_POOL_NAME = "duckdb_pool"
SALES_TABLE_NAME = "sales"
LOGGER = getLogger(__name__)


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2},
    tags=["duckdb", "csv", "demo"],
)
def duck_db_csv_ingest():
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command=(
            f"airflow pools list | grep -q '{DUCKDB_POOL_NAME}' "
            f"|| airflow pools set {DUCKDB_POOL_NAME} 1 'Pool for duckdb'"
        ),
    )

    @task(task_id="load_sales_csv", pool=DUCKDB_POOL_NAME)
    def load_sales_csv(table_name: str):
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"Could not find sample sales CSV at {CSV_PATH}")

        duckdb_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = duckdb_hook.get_conn()
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{CSV_PATH.as_posix()}', header=True);"
        )
        loaded_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        LOGGER.info("Loaded %s rows into %s from %s", loaded_rows, table_name, CSV_PATH)

    @task(task_id="log_sales_totals_by_region")
    def log_sales_totals_by_region(table_name: str):
        duckdb_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = duckdb_hook.get_conn()
        results = conn.execute(
            f"""
            SELECT
                region,
                ROUND(SUM(amount), 2) AS total_amount
            FROM {table_name}
            GROUP BY region
            ORDER BY region
            """
        ).fetchall()

        for region, total_amount in results:
            LOGGER.info("Region: %s, total_amount: %s", region, total_amount)

        if not results:
            raise ValueError(f"No sales totals were produced from table {table_name}")

        return results

    (
        create_duckdb_pool
        >> load_sales_csv(table_name=SALES_TABLE_NAME)
        >> log_sales_totals_by_region(table_name=SALES_TABLE_NAME)
    )


duck_db_csv_ingest()
