from airflow.sdk import dag,task
from pendulum import datetime,duration
from airflow.timetables.trigger import CronTriggerTimetable, DeltaTriggerTimetable

@dag(
    
    dag_id = "first_schedule_delta_dag_3days",
    start_date = datetime(year=2026, month=4, day=24, tz="Asia/Kolkata"),
    schedule = DeltaTriggerTimetable(duration(days=3,hours=4, minutes=30)),
    is_paused_upon_creation = False,
    catchup = True
)

def first_schedule_delta_dag_3days():
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

first_schedule_delta_dag_3days()