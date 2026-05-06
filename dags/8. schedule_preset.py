from airflow.sdk import dag,task
from pendulum import datetime

@dag(
    
    dag_id = "first_schedule_dag",
    start_date = datetime(year=2026, month=5, day=4, tz="Asia/Kolkata"),
    schedule = "@daily",
    is_paused_upon_creation = False,
    catchup = True
)

def first_schedule_dag():
    
    @task
    def first_task():
        print("This is first task")

    @task
    def second_task():
        print("This is second task")


    @task
    def third_task():
        print("This is third task")


    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

first_schedule_dag()