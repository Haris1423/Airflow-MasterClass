from airflow.sdk import dag,task
import os

@dag (
    dag_id="second_dag_orchestrate"
)
def second_dag_orchestrate():
    
    @task
    def first_task():
        print("This is first task")

    @task
    def second_task():
        print("This is second task")


    @task
    def third_task():
            os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok=True)
            with open("/opt/airflow/logs/data/output_2.txt", 'w') as f:
                f.write("Data processed on successfully!")


    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

second_dag_orchestrate()