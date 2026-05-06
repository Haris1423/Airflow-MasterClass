from airflow.sdk import dag,task
from airflow.providers.standard.operators.bash import BashOperator 


@dag
def XCOMS_dag_manual():
    
    @task.python
    def first_task(**kwargs):
        #Extracting TI from kwargs to push XCOM manually
        ti = kwargs["ti"]
        print("Extracting data... this is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key = 'return_result', value = fetched_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        #pulling Xcoms pushed by the first task 
        fetched_data = ti.xcom_pull(task_ids= 'first_task', key = 'return_result')['data']
        print("Transforming data... this is the second task")
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transformed_data": transformed_data}
        ti.xcom_push(key = 'return_second_result', value = transformed_data_dict )
    
    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids='second_task', key='return_second_result')

    first = first_task()
    second = second_task()  
    third = third_task() 
    first >> second >> third
 
XCOMS_dag_manual()