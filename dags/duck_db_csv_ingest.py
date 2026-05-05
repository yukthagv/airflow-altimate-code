"""
### Ingest sample sales CSV into DuckDB and aggregate totals by region

This DAG creates the shared DuckDB pool used in the project, loads the
`include/sample_sales.csv` file into a DuckDB table called `sales`, and logs
total sales amount by region. The pool serializes the write task so only one
writer updates DuckDB at a time.

Airflow 3 Migration Notes:
  - read_csv_auto() renamed to read_csv() in DuckDB >=1.0
  - BashOperator pool creation REMOVED: In Airflow 3, task workers run in
    isolated processes with no access to the metadata DB or airflow CLI.
    Pool is now created via the Airflow 3 REST API inside a @task.
  - airflow.decorators deprecated in favour of airflow.sdk — updated imports.
"""

from logging import getLogger
from pathlib import Path

from airflow.sdk import dag, task
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
    @task(task_id="create_duckdb_pool")
    def create_duckdb_pool():
        """Ensure the duckdb_pool exists via the Airflow 3 REST API.

        Airflow 3: BashOperator running 'airflow pools set' no longer works
        because task workers are isolated and cannot access the metadata DB.
        We use the REST API (localhost:8080) instead, which is always reachable
        from within the same Docker network.
        """
        import os
        import urllib.request
        import urllib.parse
        import json

        base_url = os.environ.get("AIRFLOW__API__BASE_URL", "http://localhost:8080")

        # Authenticate — get JWT token
        auth_payload = json.dumps({"username": "admin", "password": "admin"}).encode()
        req = urllib.request.Request(
            f"{base_url}/auth/token",
            data=auth_payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            token = json.loads(resp.read())["access_token"]

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Check if pool already exists
        check_req = urllib.request.Request(
            f"{base_url}/api/v2/pools/{DUCKDB_POOL_NAME}",
            headers=headers,
            method="GET",
        )
        try:
            with urllib.request.urlopen(check_req) as resp:
                pool_data = json.loads(resp.read())
                LOGGER.info(
                    "Pool '%s' already exists with %d slots",
                    DUCKDB_POOL_NAME,
                    pool_data.get("slots", 0),
                )
                return
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

        # Create the pool
        pool_payload = json.dumps(
            {
                "name": DUCKDB_POOL_NAME,
                "slots": 1,
                "description": "Pool for DuckDB — serialises writes to prevent file lock conflicts",
                "include_deferred": False,
            }
        ).encode()
        create_req = urllib.request.Request(
            f"{base_url}/api/v2/pools",
            data=pool_payload,
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(create_req) as resp:
            result = json.loads(resp.read())
            LOGGER.info(
                "Created pool '%s' with %d slot(s)", result["name"], result["slots"]
            )

    @task(task_id="load_sales_csv", pool=DUCKDB_POOL_NAME)
    def load_sales_csv(table_name: str):
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"Could not find sample sales CSV at {CSV_PATH}")

        duckdb_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        conn = duckdb_hook.get_conn()
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        # Airflow 3: read_csv_auto() -> read_csv() (DuckDB >=1.0)
        conn.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_csv('{CSV_PATH.as_posix()}', header=True);"
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
        create_duckdb_pool()
        >> load_sales_csv(table_name=SALES_TABLE_NAME)
        >> log_sales_totals_by_region(table_name=SALES_TABLE_NAME)
    )


duck_db_csv_ingest()
