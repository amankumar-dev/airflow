from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import requests


@dag(
    dag_id="user_pipeline_final",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def user_pipeline():

    # ---------- 1. SENSOR ---------- #
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get("https://dummyjson.com/users/1")

        if response.status_code == 200:
            return PokeReturnValue(
                is_done=True,
                xcom_value=response.json()
            )
        return PokeReturnValue(is_done=False, xcom_value=None)


    # ---------- 2. EXTRACT ---------- #
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["firstName"],
            "lastname": fake_user["lastName"],
            "email": fake_user["email"]
        }


    # ---------- 3. PROCESS → CSV ---------- #
    @task
    def process_user(user_info):
        import csv
        path = "/tmp/user_info.csv"

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

        return path   # 👈 important (next task ko dena hai)


    # ---------- 4. CREATE TABLE ---------- #
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


    # ---------- 5. STORE → COPY ---------- #
    @task
    def store_user(csv_path):
        hook = PostgresHook(postgres_conn_id="postgres")

        hook.copy_expert(
            sql="""
            COPY users (id, firstname, lastname, email)
            FROM STDIN WITH CSV HEADER
            """,
            filename=csv_path
        )


    # ---------- FLOW (🔥 FINAL) ---------- #
    create_table >> store_user(
        process_user(
            extract_user(
                is_api_available()
            )
        )
    )


# DAG register
user_pipeline()