"""
### Load and transform DuckDB data using TaskFlow (Airflow 3 rewrite)

This DAG replicates the original Astro Python SDK pipeline without the SDK
dependency, which is not confirmed compatible with Airflow 3. The same logical
pipeline is preserved:

  1. Load `include/ducks.csv` into a DuckDB table.
  2. Count rows in that table.
  3. Sample 3 random rows and persist them to a separate table.

Airflow 3 Migration Notes:
  - astro-sdk-python REMOVED — not confirmed compatible with Airflow 3.
  - aql.load_file       -> @task using DuckDBHook + read_csv() (DuckDB >=1.0)
  - @aql.transform      -> @task using DuckDBHook SQL execution
  - @aql.dataframe      -> @task using pandas + DuckDBHook
  - aql.cleanup()       -> removed (no temp tables to clean up)
  - duckdb_engine patch -> removed (was only needed by astro-sdk's pandas.to_sql)
  - read_csv_auto()     -> read_csv() (DuckDB >=1.0 API change)
  - BashOperator pool creation REMOVED: pool must be pre-created via CLI/UI.
    In Airflow 3, task workers are isolated and cannot call the airflow CLI
    or connect to the metadata DB.
  - airflow.decorators deprecated -> airflow.sdk (Airflow 3 stable interface)
"""

import pandas as pd
from airflow.sdk import dag, task
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from pendulum import datetime

DUCKDB_CONN_ID = "my_local_duckdb_conn"
DUCKDB_POOL_NAME = "duckdb_pool"
CSV_PATH = "include/ducks.csv"
SOURCE_TABLE = "ducks"
SAMPLE_TABLE = "three_random_ducks"


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    tags=["duckdb", "astro-sdk-replacement", "demo"],
    default_args={"retries": 2},
)
def duckdb_and_astro_sdk_example():
    @task(task_id="load_ducks", pool=DUCKDB_POOL_NAME)
    def load_ducks():
        """Load ducks.csv into DuckDB table using DuckDBHook.

        Replaces: aql.load_file(File(CSV_PATH), output_table=Table(conn_id=...))
        Uses read_csv() — renamed from read_csv_auto() in DuckDB >=1.0.
        """
        hook = DuckDBHook.get_hook(DUCKDB_CONN_ID)
        conn = hook.get_conn()
        conn.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE};")
        conn.execute(
            f"CREATE TABLE {SOURCE_TABLE} AS "
            f"SELECT * FROM read_csv('{CSV_PATH}', header=True);"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE};").fetchone()[0]
        return row_count

    @task(task_id="count_ducks")
    def count_ducks(row_count: int):
        """Log the row count loaded from ducks.csv.

        Replaces: @aql.transform running SELECT count(*) FROM {{ in_table }}
        """
        print(f"Loaded {row_count} duck species into DuckDB.")
        return row_count

    @task(task_id="select_ducks", pool=DUCKDB_POOL_NAME)
    def select_ducks():
        """Sample 3 random ducks and persist to a separate table.

        Replaces: @aql.dataframe sampling 3 random rows and writing back.
        """
        hook = DuckDBHook.get_hook(DUCKDB_CONN_ID)
        conn = hook.get_conn()
        df = conn.execute(f"SELECT * FROM {SOURCE_TABLE};").df()
        three_random_ducks = df.sample(n=min(3, len(df)), replace=False)
        print(three_random_ducks)
        conn.execute(f"DROP TABLE IF EXISTS {SAMPLE_TABLE};")
        conn.execute(
            f"CREATE TABLE {SAMPLE_TABLE} AS SELECT * FROM three_random_ducks;"
        )
        return len(three_random_ducks)

    # Wire up the pipeline — load first, then count and sample in parallel
    loaded = load_ducks()
    count_ducks(loaded)
    select_ducks()


duckdb_and_astro_sdk_example()
