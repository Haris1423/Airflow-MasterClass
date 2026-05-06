from dag_orchestrator_1 import first_orchestrator_dag
from dag_orchestrator_2 import second_orchestrator_dag
from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(dag_id="parent_orchestrator_dag",
    is_paused_upon_creation=False)
def parent_orchestrator_dag():

    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestrator_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestrator_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True
    )

    trigger_first_dag >> trigger_second_dag

parent_orchestrator_dag()