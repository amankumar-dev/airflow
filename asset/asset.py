from airflow.sdk import asset

@asset(schedule="@daily")
def user():
    import requests
    return requests.get("https://randomuser.me/api/").json()


@asset(schedule=[user])
def user_location(user_data):
    return user_data['results'][0]['location']


@asset(schedule=[user])
def user_login(user_data):
    return user_data['results'][0]['login']