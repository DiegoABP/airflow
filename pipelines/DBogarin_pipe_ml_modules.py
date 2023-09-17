from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, accuracy_score
import logging
import joblib
import numpy as np


def load_data(table_id,uri):
    client = bigquery.Client();

    table = client.get_table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=table.schema,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.field_delimiter = ";"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

def preprocess_data(project_id, dataset_id, input_table_id, output_table_id):
    
    client = bigquery.Client(project=project_id)
    input_query = f"SELECT * FROM `{project_id}.{dataset_id}.{input_table_id}`"
    df = client.query(input_query).to_dataframe()

    # Preprocesar los datos
    df = df.drop(columns=['Skewness','EK','Skewness_DMSNR_Curve','EK_DMSNR_Curve'])
    missing_values = df.isna().sum()
    df= len(missing_values[missing_values > 0].sort_values() )
    
    df_imputed = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)
    df_imputed['Class'] = df_imputed['Class'].round(0)
 
    # Vaciar la tabla de salida y cargar los datos preprocesados
    client.delete_table(f'{project_id}.{dataset_id}.{output_table_id}', not_found_ok=True)
    client.load_table_from_dataframe(df_imputed, f'{project_id}.{dataset_id}.{output_table_id}').result()
    
def ml_training_RandomForest_bigquery(project_id, dataset_id, table_id,**kwargs):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()

    # Dividir el DataFrame final en X e y
    X = final.iloc[:, 1:]
    y = final.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = RandomForestClassifier(n_estimators=100)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
    
def ml_training_logistic_regression_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()

    # Dividir el DataFrame final en X e y
    X = final.iloc[:, 1:]
    y = final.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = LogisticRegression(max_iter=100)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
    
def ml_training_gradient_boosting_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()

    # Dividir el DataFrame final en X e y
    X = final.iloc[:, 1:]
    y = final.iloc[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = GradientBoostingClassifier(n_estimators=100)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
    
