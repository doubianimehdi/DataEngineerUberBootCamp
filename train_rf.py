import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# Ne pas oublier d'installer seaborn et matplotlib sur la VM d'Airflow
import seaborn as sns

# Ne pas oublier d'installer mlflow sur la VM d'Airflow
import mlflow
import mlflow.sklearn

# Authentification à Google Cloud avec la clé correspondant au compte de service MLflow
import os

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

from google.cloud import storage

EXPERIMENT_ID = 1 # Choisir le bon ID

# Ne pas oublier d'ajouter la clé pour Storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\blent-data-engineer\storage-key.json'

sns.set(rc={'figure.figsize':(14,10)})
# TODO : À modifier
project_id = "data-engineer-duration"
mlflow.set_tracking_uri("http://34.65.91.126:5000/")
client = storage.Client()

def mae(y_star, y_pred):
    return 1 / len(y_star) * np.sum(np.abs(y_star - y_pred))

def plot_mae(X, y, model):
    """
    Il est aussi pertinent de logger les graphiques sous forme d'artifacts.
    """
    fig = plt.figure()
    plt.scatter(y, model.predict(X))
    plt.xlabel("Durée réelle du trajet")
    plt.ylabel("Durée estimée du trajet")
    
    image = fig
    fig.savefig("MAE.png")
    plt.close(fig)
    return image


def load_data(path):
    """
    Charge les données et retourne la base d'apprentissage (X, y).
    """
    data = pd.read_csv(path)
    data['pickup_datetime'] = pd.to_datetime(data['pickup_datetime'])
    data['dropoff_datetime'] = pd.to_datetime(data['dropoff_datetime'])

    # Création des colonnes liés au pickup
    data['pickup_hour'] = data['pickup_datetime'].dt.hour
    data['pickup_minute'] = data['pickup_datetime'].dt.minute
    data['pickup_second'] = data['pickup_datetime'].dt.second
    # Durée réelle du trajet en secondes
    data['duration'] = (data['dropoff_datetime'] - data['pickup_datetime']).dt.seconds
    data_base = data[(data['duration'] < 3600) & (data['duration'] > 60)]

    X = data_base[['trip_distance', 'PULocationID', 'DOLocationID', 'pickup_hour',
            'pickup_minute', 'pickup_second']]
    y = data_base['duration']

    return X, y

def train_model(path):
    """
    Entraîne le modèle et envoie les métriques et artifacts sur Mlflow.
    """
    X, y = load_data(path)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

    n_estimators = 50
    max_depth = 12
 
    with mlflow.start_run(experiment_id=EXPERIMENT_ID):
        rf = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth)
        rf.fit(X_train, y_train)  # Processus d'optimisation de l'arbre

        mae_score = mae(y_test, rf.predict(X_test))
        plot_mae(X_test, y_test, rf)

        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)

        mlflow.log_metric("mae", mae_score)
        mlflow.log_artifact("MAE.png")
        mlflow.sklearn.log_model(rf, "model")