"""
### Use Airflow together with a local DuckDB database

This DAG shows examples of how to interact with a local DuckDB database from
within TaskFlow tasks. It exercises both in-memory DuckDB and a persistent
DuckDB file stored under `include/my_local_ducks.db`.

The tasks that write to the persistent file are chained so only one writer
holds the DuckDB file lock at a time.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import duckdb
import pandas as pd

CSV_PATH = "include/ducks.csv"
LOCAL_DUCKDB_STORAGE_PATH = "include/my_local_ducks.db"


@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_in_taskflow():
    @task
    def create_table_in_memory_db_1():
        "Create and query a temporary in-memory DuckDB database."

        in_memory_duck_table_1 = duckdb.sql(
            f"SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"
        )
        duck_species_count = duckdb.sql(
            "SELECT count(*) FROM in_memory_duck_table_1;"
        ).fetchone()[0]
        return duck_species_count

    @task
    def create_table_in_memory_db_2():
        "Create and query a temporary in-memory DuckDB database."

        conn = duckdb.connect()
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS in_memory_duck_table_2 AS
            SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"""
        )
        duck_species_count = conn.sql(
            "SELECT count(*) FROM in_memory_duck_table_2;"
        ).fetchone()[0]
        return duck_species_count

    @task
    def create_pandas_df():
        "Create a pandas DataFrame with toy data and return it."
        ducks_in_my_garden_df = pd.DataFrame(
            {"colors": ["blue", "red", "yellow"], "numbers": [2, 3, 4]}
        )

        return ducks_in_my_garden_df

    @task
    def create_table_from_pandas_df(ducks_in_my_garden_df, local_duckdb_storage_path):
        "Create a table in a local DuckDB file from a pandas DataFrame."

        conn = duckdb.connect(local_duckdb_storage_path)
        conn.sql(
            """CREATE TABLE IF NOT EXISTS ducks_garden AS
            SELECT * FROM ducks_in_my_garden_df;"""
        )

    @task
    def create_table_in_local_persistent_storage(local_duckdb_storage_path):
        "Create a table in a local persistent DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS persistent_duck_table AS
            SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"""
        )
        duck_species_count = conn.sql(
            "SELECT count(*) FROM persistent_duck_table;"
        ).fetchone()[0]
        return duck_species_count

    @task
    def query_persistent_local_storage(local_duckdb_storage_path):
        "Query a table in a local persistent DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        species_with_blue_in_name = conn.sql(
            """SELECT species_name FROM persistent_duck_table
            WHERE species_name LIKE '%Blue%';"""
        ).fetchall()

        blue_df = pd.DataFrame(species_with_blue_in_name)

        conn.sql(
            """CREATE TABLE IF NOT EXISTS blue_ducks AS
            SELECT * FROM blue_df;"""
        )
        return species_with_blue_in_name

    @task
    def csv_file_to_local_duckdb(local_duckdb_storage_path):
        "Load data from a CSV file into a table in a local DuckDB database."

        conn = duckdb.connect(local_duckdb_storage_path)
        conn.sql(
            f"""CREATE TABLE IF NOT EXISTS duck_species_table AS
            SELECT * FROM read_csv_auto('{CSV_PATH}', header=True);"""
        )
        duck_species_count = conn.sql(
            "SELECT count(*) FROM duck_species_table;"
        ).fetchone()[0]
        return duck_species_count

    @task
    def print_count(duck_species_count):
        print(f"Duck species count: {duck_species_count}")

    print_count(create_table_in_memory_db_1())
    print_count(create_table_in_memory_db_2())

    persistent_create = create_table_in_local_persistent_storage(
        local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
    )
    persistent_query = query_persistent_local_storage(
        local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
    )
    csv_count = csv_file_to_local_duckdb(
        local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
    )
    pandas_write = create_table_from_pandas_df(
        create_pandas_df(), local_duckdb_storage_path=LOCAL_DUCKDB_STORAGE_PATH
    )

    persistent_create >> persistent_query >> csv_count >> print_count(csv_count) >> pandas_write


duckdb_in_taskflow()
