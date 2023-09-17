from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pipe_ml_modules import ml_training_gradient_boosting_bigquery
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Definir los argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define el DAG
with DAG('DBogarin_pipe_ml_dag_training_gradient_boosting',
         default_args=default_args,
         description='gradient boosting',
         schedule_interval=None,
         ) as dag:

    # Define la tarea
    ml_training_gradient_boosting = PythonOperator(
        task_id='ml_training_gradient_boosting',
        python_callable=ml_training_gradient_boosting_bigquery,
        op_args=['automatic-tract-396023', 'mlairflow', 'mlairflow_process'],
    )

    # Establecer las dependencias
    ml_training_gradient_boosting
