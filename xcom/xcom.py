from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime


@dag(start_date=datetime(2024,1,1), schedule=None, catchup=False)
def xcom_demo():

    # ---------------- AUTO XCOM (MODERN) ---------------- #
    with TaskGroup("auto_xcom_group"):

        @task
        def task_a():
            return 10   # 🔥 auto XCom push

        @task
        def task_b(x):
            print("Auto received:", x)

        x = task_a()
        task_b(x)   # 🔥 auto XCom pull


    # ---------------- MANUAL XCOM (OLD) ---------------- #
    with TaskGroup("manual_xcom_group"):

        @task
        def task_c(**context):
            context['ti'].xcom_push(key="my_value", value=20)  # manual push

        @task
        def task_d(**context):
            val = context['ti'].xcom_pull(task_ids="manual_xcom_group.task_c", key="my_value")
            print("Manual received:", val)

        task_c() >> task_d()


xcom_demo()