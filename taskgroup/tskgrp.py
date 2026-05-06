from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "taskgroup", "clean"]
)
def user_etl_pipeline():

    # ----------- EXTRACT ----------- #
    with TaskGroup("extract") as extract:

        @task
        def fetch_user():
            import requests
            return requests.get("https://randomuser.me/api/").json()

        user_data = fetch_user()   # 👈 call inside group


    # ----------- TRANSFORM ----------- #
    with TaskGroup("transform") as transform:

        @task
        def clean(user_data):
            return user_data["results"][0]

        cleaned = clean(user_data)

        # 🔥 Nested group (feature engineering)
        with TaskGroup("features") as features:

            @task
            def get_location(data):
                return data["location"]

            @task
            def get_email(data):
                return data["email"]

            location = get_location(cleaned)
            email = get_email(cleaned)


    # ----------- LOAD ----------- #
    with TaskGroup("load") as load:

        @task
        def save_csv(location, email):
            import csv
            with open("/tmp/user.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["location", "email"])
                writer.writerow([location, email])

        save = save_csv(location, email)


    # ----------- GROUP DEPENDENCIES ----------- #
    extract >> transform >> load


user_etl_pipeline()