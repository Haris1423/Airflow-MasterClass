from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable


special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=5, day=1, tz="UTC"),
        datetime(year=2026, month=5, day=3, tz="UTC"),
        datetime(year=2026, month=5, day=5, tz="UTC"),
        datetime(year=2026, month=5, day=7, tz="UTC")
    ]
)


@dag(
    dag_id="special_schedule_dag",
    schedule=special_dates,
    start_date=datetime(year=2026, month=5, day=1, tz="UTC"),
    end_date=datetime(year=2026, month=5, day=10, tz="UTC"),
    catchup=True,
    is_paused_upon_creation=False
)
def special_schedule_dag():

    @task
    def special_task():
        print("Executing task for special scheduled date...")

    special_task()


special_schedule_dag()