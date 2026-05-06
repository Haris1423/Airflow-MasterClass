# dags/main.py
from airflow.sdk import asset

@asset(schedule="@daily")
def task1():
    return {
        "name": "Taro Tanaka",
        "score": 75,
        "update_date": "2025-05-01"
    }

@asset(schedule=task1)
def task2(context):
    task1_data = context["ti"].xcom_pull(
        task_ids="task1",
        key="return_value",
        include_prior_dates=True,
    )
    score = task1_data["score"]
    print(f"[task2] The score is: {score}")
    return "pass" if score >= 80 else "fail"

@asset(schedule=task2)
def task3(context):
    result = context["ti"].xcom_pull(
        task_ids="task2",
        key="return_value",
        include_prior_dates=True,
    )
    if result == "pass":
        print("✅ Passed!")
    else:
        print("Skipped (task3)")

@asset(schedule=task2)
def task4(context):
    result = context["ti"].xcom_pull(
        task_ids="task2",
        key="return_value",
        include_prior_dates=True,
    )
    if result == "fail":
        print("❌ Failed.")
    else:
        print("Skipped (task4)")