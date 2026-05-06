from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator 


@dag
def XCOMS_dag_auto():
    
    @task
    def first_task():
        print("Extracting data... this is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def second_task(data : dict):
        fetched_data = data['data']
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transformed_data": transformed_data}
        return transformed_data_dict
    
    @task.python
    def third_task(data : dict):
        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)  
    third = third_task(second) 
 
XCOMS_dag_auto()