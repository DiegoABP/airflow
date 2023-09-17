from google.cloud import bigquery
import pandas as pd
from sklearn.model_selection import train_test_split    
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer

def ml_training_RandomForest_bigquery(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
    final = client.query(query).to_dataframe()
    
    # Preprocesar los datos
    # Eliminar columnas no numéricas
    final = final.drop(columns=['Name', 'Ticket', 'Cabin', 'Embarked'])
    # Codificar la columna 'Sex' como 0 o 1
    final['Sex'] = final['Sex'].apply(lambda x: 1 if x == 'male' else 0)
    
    # Imputar valores faltantes
    imputer = SimpleImputer(strategy='mean')
    final_imputed = imputer.fit_transform(final)

    # Dividir el DataFrame final en X e y
    X = final_imputed[:, 1:]
    y = final_imputed[:, 0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    clf = RandomForestClassifier(n_estimators=100)
    clf.fit(X_train, y_train)
  
    y_pred = clf.predict(X_test)
  
    acc = accuracy_score(y_test, y_pred)
    return {'model_accuracy': acc}
