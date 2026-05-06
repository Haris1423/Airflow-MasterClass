from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id="myfirst_dag",
    start_date=datetime(2026, 5, 2),
    #schedule='*/2 * * * *',
    catchup=False
)

def printContext(**kwargs):
    print(kwargs)
    print("This is task 2")

copy_file = BashOperator(dag=dag,
                        task_id="copy_file",
                        bash_command="echo Copying file...")

task2 = PythonOperator(dag=dag,
                       task_id="task2",
                       python_callable=printContext)

copy_file >> task2
# copy_file.set_downstream(task2)
