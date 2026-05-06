from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task


@dag(
    dag_id="task_flow_api_dag",
    start_date=datetime(2026, 5, 2),
    schedule="@daily",
    catchup=False    
)

# dag = DAG(
#     dag_id="task_flow_api_dag",
#     start_date=datetime(2026, 5, 2),
#     schedule="@daily",
#     catchup=False)

def task_flow_api_dag():
    @task
    def extract():
        return {"output_path": "/path/to/data.csv"}

    @task
    def transform(extracted_data):
        input_path = extracted_data["output_path"]
        print(f"Received extracted data with output_path: {input_path}")

    def load():
        print("Loading data...")

    load = PythonOperator(
        task_id="load",python_callable=load
    )

    transform(extract()) >> load

task_flow_api_dag()

# def extract(**context):
#     ti = context['ti']
#     ti.xcom_push(key="output_path", value="/path/to/data.csv")
#     print("Pushed Xcom with key 'output_path' and value '/path/to/data.csv'")

# def transform(**context):
#     ti = context['ti']
#     extracted_data = ti.xcom_pull(key="output_path", task_ids="extract")
#     input_path = extracted_data["output_path"]
#     print(f"Pulled Xcom with key 'output_path' from task 'extract': {extracted_data}")

# extract_task = PythonOperator(dag=dag,
#                               task_id="extract",
#                               python_callable=extract)

# transform_task = PythonOperator(dag=dag,
#                                task_id="transform", 
#                                python_callable=transform)

# extract_task >> transform_task