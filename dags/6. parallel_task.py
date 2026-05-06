from airflow.sdk import dag,task

@dag
def parallel_dag():
    
    @task
    def extract_data(**kwargs):
        print("Extracted Data ......")
        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data" : [1, 2, 3],
                               "db_extracted_data" : [4, 5, 6],
                               "s3_extracted_data" : [7, 8, 9]
                               }
        ti.xcom_push(key='return_extracted', value = extracted_data_dict)

    @task.python
    def trasnform_api_data(**kwargs):
        ti = kwargs['ti']
        api_extracted_data= ti.xcom_pull(task_ids='extract_data', key = 'return_extracted')['api_extracted_data']
        print(f"transforming API data :{api_extracted_data} ")
        trasnformed_api_data = [i *10 for i in api_extracted_data]
        ti.xcom_push(key ='transformed_api_data', value = trasnformed_api_data)

    @task.python
    def trasnform_db_data(**kwargs):
        ti = kwargs['ti']
        db_extracted_data= ti.xcom_pull(task_ids='extract_data', key = 'return_extracted')['db_extracted_data']
        print(f"transforming DB data :{db_extracted_data} ")
        trasnformed_db_data = [i *100 for i in db_extracted_data]
        ti.xcom_push(key ='transformed_db_data', value = trasnformed_db_data)   

    @task.python
    def trasnform_s3_data(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data= ti.xcom_pull(task_ids='extract_data', key = 'return_extracted')['s3_extracted_data']
        print(f"transforming S3 data :{s3_extracted_data} ")
        trasnformed_s3_data = [i * 1000 for i in s3_extracted_data]
        ti.xcom_push(key ='transformed_s3_data', value = trasnformed_s3_data)                             

    @task.bash
    def load_task(**kwargs):
        print("Loading data to destination.....")
        api_data = kwargs['ti'].xcom_pull(task_ids='trasnform_api_data', key = 'transformed_api_data')
        db_data = kwargs['ti'].xcom_pull(task_ids='trasnform_db_data', key = 'transformed_db_data')
        s3_data = kwargs['ti'].xcom_pull(task_ids='trasnform_s3_data', key = 'transformed_s3_data')
        return f"echo 'Loaded data : {api_data} , {db_data} , {s3_data}'"
        

    extract = extract_data()
    trasnform_api = trasnform_api_data()
    trasnform_db = trasnform_db_data()
    trasnform_s3 = trasnform_s3_data()
    load = load_task()

    extract >> [trasnform_api, trasnform_db, trasnform_s3] >> load

parallel_dag()