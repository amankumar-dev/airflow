from airflow.sdk import dag, task
from datetime import datetime

@dag(start_date=datetime(2024,1,1), schedule=None, catchup=False)
def branching_example():

    @task
    def check_value():
        return 5

    @task.branch
    def choose_path(x):
        if x > 3:
            return "task_high"
        else:
            return "task_low"

    @task
    def task_high():
        print("High value path")

    @task
    def task_low():
        print("Low value path")

    x = check_value()
    decision = choose_path(x)

    decision >> [task_high(), task_low()]

branching_example()