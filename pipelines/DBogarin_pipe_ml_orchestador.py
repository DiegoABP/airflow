from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pipe_ml_modules import load_data,ml_training_RandomForest_bigquery,ml_training_logistic_regression_bigquery,ml_training_gradient_boosting_bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
}

with DAG('DBogarin_pipe_ml_orchestador_dags',
         default_args=default_args,
         schedule_interval='0 23 * * *',
         ) as dag:
         
    inicio = DummyOperator(
            task_id='Inicio_proceso',
        )
        
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=["automatic-tract-396023.mlairflow.data_titanic","Pulsar.csv"],
    )    

    preprocess = TriggerDagRunOperator(
        task_id='preprocess',
        trigger_dag_id='pipe_ml_dag_preprocess',
        )
        
    with TaskGroup('Model_Training') as Model_Training:
    
        join_start = DummyOperator(
            task_id='start',
        )
        """
        training_gradient_boosting = TriggerDagRunOperator(
        task_id='gradient_boosting',
        trigger_dag_id='pipe_ml_dag_training_gradient_boosting',
        )
        logistic_regression = TriggerDagRunOperator(
        task_id='logistic_regression',
        trigger_dag_id='pipe_ml_dag_training_logistic_regression',
        )
        random_forest = TriggerDagRunOperator(
        task_id='random_forest',
        trigger_dag_id='pipe_ml_dag_training_randomforest',
        )
        """
        
        # Define la tarea
        ml_training_randomforest = PythonOperator(
            task_id='DBogarin_ml_training_randomforest',
            python_callable=ml_training_RandomForest_bigquery,
            op_args=['automatic-tract-396023', 'mlairflow', 'mlairflow_process'],
        )
        
        # Define la tarea
        ml_training_logistic_regression = PythonOperator(
            task_id='DBogarin_ml_training_logistic_regression',
            python_callable=ml_training_logistic_regression_bigquery,
            op_args=['automatic-tract-396023', 'mlairflow', 'mlairflow_process'],
        )
        
        # Define la tarea
        ml_training_gradient_boosting = PythonOperator(
            task_id='DBogarin_ml_training_gradient_boosting',
            python_callable=ml_training_gradient_boosting_bigquery,
            op_args=['automatic-tract-396023', 'mlairflow', 'mlairflow_process'],
        )

        join_task = DummyOperator(
            task_id='join',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        #join_start >> [training_gradient_boosting, logistic_regression, random_forest] >> join_task
        join_start >> [ml_training_randomforest, ml_training_logistic_regression, ml_training_gradient_boosting] >> join_task         

    

    
    fin = DummyOperator(
            task_id='Fin_proceso',
        )

    inicio >>load_data>>preprocess >> Model_Training >> fin
