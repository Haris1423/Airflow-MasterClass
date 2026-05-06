from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator 


@dag
def operators_dag():
    
    @task
    def first_task():
        print("This is first task")

    @task
    def second_task():
        print("This is second task")


    @task.bash
    def run_after_version():
        return "echo 'This is a bash task that runs after version task'"
    


    @task
    def version_task():
        print("This is version task")

    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "This is a bash task that runs after version task"'
    )



    first = first_task()
    second = second_task()  
    loop = run_after_version()
    version = version_task()

    first >> second >> version >> loop >> bash_task

operators_dag()