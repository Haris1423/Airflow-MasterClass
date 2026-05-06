from airflow.sdk import dag,task

@dag
def first_dag():
    
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

first_dag()