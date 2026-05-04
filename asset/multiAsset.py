from airflow.sdk import asset

# ---------- 1. SOURCE ASSET ---------- #
@asset(schedule="@daily")
def user():
    import requests
    return requests.get("https://randomuser.me/api/").json()


# ---------- 2. MULTI-ASSET ---------- #
@asset.multi(
    schedule=[user],
    outlets=["user_location", "user_login"]
)
def user_info(user_data):
    result = user_data['results'][0]

    return {
        "user_location": result['location'],
        "user_login": result['login']
    }