from airflow.sdk import dag,task

@dag
def versioned_dag():
    
    @task
    def first_task():
        print("This is first task")

    @task
    def second_task():
        print("This is second task")


    @task
    def third_task():
        print("This is third task")

    @task
    def version_task():
        print("This is version task")


    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()

    first >> second >> version >> third

versioned_dag()