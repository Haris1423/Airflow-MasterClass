from airflow.sdk import dag, task
import os


@dag(dag_id="second_orchestrator_dag",
    is_paused_upon_creation=False)

def second_orchestrator_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")
    
    @task.python
    def third_task():
        os.makedirs(os.path.dirname("/opt/airflow/logs/data/output_2.txt"), exist_ok=True)
        with open("/opt/airflow/logs/data/output_2.txt", "w") as f:
            f.write("This is the output of the second orchestrator dag")

    first = first_task()
    second = second_task()
    third_task = third_task()

    first >> second >> third_task

second_orchestrator_dag()