from airflow.sdk.bases import xcom
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime
import pendulum
import requests
import json



dag = DAG(
    dag_id="xcom_dag",
    start_date=datetime(2026, 4, 25),
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=pendulum.timezone("UTC")),
    #catchup=True
)

def product_page_callable(**context):
    ti = context['ti']
    ti.xcom_push(key="output_path", value="/opt/airflow/output_files/folder1/test.csv")
    print("Pushed Xcom with key 'output_path' and value '/opt/airflow/output_files/folder1/test.csv'")

product_page = PythonOperator(dag=dag,
                              task_id="product_page",
                              python_callable=product_page_callable)

def read_raw_callable(**context):
    ti = context['ti']
    output_path = ti.xcom_pull(key="output_path", task_ids="product_page")
    print(f"Pulled Xcom with key 'output_path' from task 'product_page': {output_path}")

read_raw = PythonOperator(dag=dag,
                          task_id="read_raw",
                          python_callable=read_raw_callable)

product_page >> read_raw