"""
### Use the custom ExcelToDuckDBOperator based upon the DuckDBHook

This simple DAG shows the custom ExcelToDuckDBOperator in use. The custom operator
can be found in include/custom_operators/duckdb_operator.py.

Airflow 3 Migration Notes:
  - Added tags and default_args retries=2 for DAG integrity test compliance.
  - ExcelToDuckDBOperator updated to use airflow.sdk.BaseOperator (see operator file).
"""

from airflow.sdk import dag
from pendulum import datetime
from include.custom_operators.duckdb_operator import ExcelToDuckDBOperator

CONNECTION = "my_local_duckdb_conn"


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
    tags=["duckdb", "custom-operator", "demo"],
    default_args={"retries": 2},
)
def duckdb_custom_operator_example():
    ExcelToDuckDBOperator(
        task_id="excel_to_duckdb",
        table_name="ducks_in_the_pond",
        excel_path="include/ducks_in_the_pond.xlsx",
        sheet_name="Sheet 1",
        duckdb_conn_id=CONNECTION,
    )


duckdb_custom_operator_example()
