from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ml_training_module import ml_training_RandomForest_bigquery
from datetime import datetime, timedelta

# Definir los argumentos predeterminados
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG
with DAG('DBogarin_ml_training_rf_bigquery',s
         default_args=default_args,
         description='Un DAG de entrenamiento de modelo simple con Random Forest',
         schedule_interval=timedelta(days=1),
         ) as dag:

    # Define la tarea
    ml_training = PythonOperator(
        task_id='ml_training',
        python_callable=ml_training_RandomForest_bigquery,
        op_args=['automatic-tract-396023', 'mlairflow', 'data_titanic'],
    )

    # Establecer las dependencias
    ml_training
