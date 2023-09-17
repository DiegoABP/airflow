from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pipe_ml_modules import preprocess_data
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Definir los argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define el DAG
with DAG('DBogarin_pipe_ml_dag_preprocess',
         default_args=default_args,
         description='Fase de pre procesamiento',
         schedule_interval=None,
         ) as dag:

    # Define la tarea
    task_ml_preprocess = PythonOperator(
        task_id='ml_preprocess',
        python_callable=preprocess_data,
        op_args=['automatic-tract-396023', 'mlairflow', 'data_titanic', 'mlairflow_process'],
    )

    # Establecer las dependencias
    task_ml_preprocess
