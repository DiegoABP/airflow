from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'DBogarin',
    'start_date': datetime(2023, 9, 1),
}

def initiate_variable():
    return 2

def potencia(value, **kwargs):
    return pow(value,value) 

def raiz_n(value, **kwargs):
    return pow(value,1/2)

def branch(value, **kwargs):
    if value > 100:
        return 'raiz_n_in_branch'
    else:
        return 'potencia_in_branch'


with DAG('task_group_and_branching_dag',
         default_args=default_args,
         schedule_interval=None) as dag:

    initiate_task = PythonOperator(
        task_id='initiate_variable',
        python_callable=initiate_variable,
        do_xcom_push=True,
    )


    potencia_task = PythonOperator(
        task_id=' potencia_task',
        python_callable=potencia,
        op_args=[initiate_task.output],
        do_xcom_push=True,
    )

    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=branch,
        op_args=[potencia_task.output],
    )

    potencia_in_branch = PythonOperator(
        task_id='potencia_in_branch',
        python_callable=potencia,
        op_args=[potencia_task.output],
        do_xcom_push=True,
    )

    raiz_n_in_branch = PythonOperator(
        task_id='raiz_n_in_branch',
        python_callable=raiz_n,
        op_args=[potencia_task.output],
        do_xcom_push=True,
    )

    join_task = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    potencia_task >> branching_task >> [potencia_in_branch, raiz_n_in_branch] >> join_task
