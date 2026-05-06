from airflow.sdk import dag, task
from datetime import datetime


@dag(start_date=datetime(2024,1,1), schedule=None, catchup=False)
def short_circuit_full():

    # Step 1: condition check
    @task.short_circuit
    def is_valid():
        value = 2
        print("Checking condition...")

        if value > 5:
            return True   # ✅ aage sab chalega
        else:
            return False  # ❌ aage sab skip

    # Step 2: tasks that depend on condition
    @task
    def task_a():
        print("Task A running")

    @task
    def task_b():
        print("Task B running")

    @task
    def task_c():
        print("Task C running")

    # Flow
    condition = is_valid()

    condition >> [task_a(), task_b(), task_c()]


short_circuit_full()