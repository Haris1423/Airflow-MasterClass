

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime
import pendulum
import requests
import json



dag = DAG(
    dag_id="API_Incemental_Data_Load",
    start_date=datetime(2026, 4, 25),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=pendulum.timezone("UTC")),
    catchup=True
)

def fetch_api_data(**context):
    url = context['templates_dict']['url']
    output_path = context['templates_dict']['output_path']
    start_date = context['ds']
    end_date = context['ds']
    payload = json.dumps({
          "start_date": start_date,
          "end_date": end_date
        })

    headers = {
      'accept': 'application/json',
      'Authorization': 'Basic YWRtaW46bWFuaXNo',
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    data = response.json()


    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Data saved to {output_path}")

pull_api_data = PythonOperator(dag=dag,
                              task_id="pull_api_data",
                              python_callable=fetch_api_data,
                              templates_dict={
                                  "url": "http://fastapi-app:5000/getAll",
                                  "output_path": "/opt/airflow/output_files/dag_result_{{ ds }}.json"
                                }
                              )