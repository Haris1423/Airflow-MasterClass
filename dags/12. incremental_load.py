from airflow.sdk import dag,task
from pendulum import datetime,duration
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    schedule = CronDataIntervalTimetable('@daily', timezone="utc"),
    start_date = datetime(year=2026, month=5, day=1, tz="utc"),
    end_date = datetime(year=2026, month=5, day=5, tz="utc"),
    catchup = True,
    is_paused_upon_creation = False
)

def incremental_load_dag():
    
    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs["data_interval_start"]
        date_interval_end = kwargs["data_interval_end"]
        print(f"Fetching incremental data from {date_interval_start} to {date_interval_end}....")


    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}....'"
    


    incremental_data_fetch() >> incremental_data_process()

incremental_load_dag()
        