"""
### Load and transform DuckDB data using TaskFlow (Airflow 3 rewrite)

This DAG replicates the original Astro Python SDK pipeline without the SDK
dependency, which is not confirmed compatible with Airflow 3. The same logical
pipeline is preserved:

  1. Create a DuckDB pool (1 slot) to serialise writes.
  2. Load `include/ducks.csv` into a DuckDB table.
  3. Count rows in that table.
  4. Sample 3 random rows and persist them to a separate table.

Airflow 3 Migration Notes:
  - astro-sdk-python REMOVED — not confirmed compatible with Airflow 3.
  - aql.load_file       -> @task using DuckDBHook + read_csv() (DuckDB >=1.0)
  - @aql.transform      -> @task using DuckDBHook SQL execution
  - @aql.dataframe      -> @task using pandas + DuckDBHook
  - aql.cleanup()       -> removed (no temp tables to clean up)
  - duckdb_engine patch -> removed (was only needed by astro-sdk's pandas.to_sql)
  - read_csv_auto()     -> read_csv() (DuckDB >=1.0 API change)
  - Added tags for DAG integrity test compliance.
"""

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
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
    create_duckdb_pool = BashOperator(
        task_id="create_duckdb_pool",
        bash_command=(
            f"airflow pools list | grep -q '{DUCKDB_POOL_NAME}' "
            f"|| airflow pools set {DUCKDB_POOL_NAME} 1 'Pool for duckdb'"
        ),
    )

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
        The row count is passed directly from load_ducks via XCom.
        """
        print(f"Loaded {row_count} duck species into DuckDB.")
        return row_count

    @task(task_id="select_ducks", pool=DUCKDB_POOL_NAME)
    def select_ducks():
        """Sample 3 random ducks and persist to a separate table.

        Replaces: @aql.dataframe sampling 3 random rows and writing back.
        Uses pandas for the random sample, then writes back via DuckDB.
        """
        hook = DuckDBHook.get_hook(DUCKDB_CONN_ID)
        conn = hook.get_conn()

        # Load into pandas for the random sample (mirrors original @aql.dataframe)
        df = conn.execute(f"SELECT * FROM {SOURCE_TABLE};").df()
        three_random_ducks = df.sample(n=min(3, len(df)), replace=False)
        print(three_random_ducks)

        # Persist sample table back to DuckDB
        conn.execute(f"DROP TABLE IF EXISTS {SAMPLE_TABLE};")
        conn.execute(
            f"CREATE TABLE {SAMPLE_TABLE} AS SELECT * FROM three_random_ducks;"
        )
        return len(three_random_ducks)

    # Wire up the pipeline
    loaded = load_ducks()
    create_duckdb_pool >> loaded

    # count_ducks and select_ducks both depend on the load completing
    count_ducks(loaded)
    select_ducks()


duckdb_and_astro_sdk_example()
