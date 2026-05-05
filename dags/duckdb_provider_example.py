"""
### Use the DuckDB provider to connect to a local DuckDB database

This DAG uses the DuckDBHook to connect to a local DuckDB database via the
`my_local_duckdb_conn` Airflow connection. A setup task seeds the table from
`include/ducks.csv` so the query task has data to read on a fresh database.

Airflow 3 Migration Notes:
  - read_csv_auto() renamed to read_csv() in DuckDB >=1.0
  - Added tags and default_args retries=2 to satisfy DAG integrity checks
  - DuckDBHook import path unchanged; provider declares apache-airflow>=2.0
    which does not hard-block Airflow 3
"""

from airflow.sdk import dag, task
from pendulum import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

CSV_PATH = "include/ducks.csv"
LOCAL_DUCKDB_CONN_ID = "my_local_duckdb_conn"
LOCAL_DUCKDB_TABLE_NAME = "ducks_table"


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    tags=["duckdb", "provider", "demo"],
    default_args={"retries": 2},
)
def duckdb_provider_example():
    @task
    def seed_local_duckdb(my_table):
        my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = my_duck_hook.get_conn()
        # Airflow 3: read_csv_auto() -> read_csv() (DuckDB >=1.0)
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {my_table} AS "
            f"SELECT * FROM read_csv('{CSV_PATH}', header=True);"
        )

    @task
    def query_local_duckdb(my_table):
        my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = my_duck_hook.get_conn()

        r = conn.execute(f"SELECT * FROM {my_table};").fetchall()
        print(r)

        return r

    seed_local_duckdb(my_table=LOCAL_DUCKDB_TABLE_NAME) >> query_local_duckdb(
        my_table=LOCAL_DUCKDB_TABLE_NAME
    )


duckdb_provider_example()
