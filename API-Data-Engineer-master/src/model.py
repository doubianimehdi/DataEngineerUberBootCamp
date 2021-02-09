# Ne pas oublier d'installer mlflow sur la VM d'Airflow
import mlflow
import mlflow.sklearn

import sklearn.ensemble

# Authentification à Google Cloud avec la clé correspondant au compte de service MLflow
import os

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

from google.cloud import storage

EXPERIMENT_ID = 1  # Choisir le bon ID

# Ne pas oublier d'ajouter la clé pour Storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './data/key.json'

# TODO : Inscrire la bonne URL du tracking MLflow
mlflow.set_tracking_uri("http://34.65.91.126:5000/")

model = None

def load_model():
    global model
    # On récupère la liste des runs pour l'expérience
    runs = mlflow.search_runs(experiment_ids=[EXPERIMENT_ID])

    # On récupère le modèle associé au run le plus récent (le premier)
    if runs.shape[0] > 0:
        run_id = runs.loc[0, 'run_id']
        model = mlflow.sklearn.load_model("runs:/{}/model".format(run_id))

def predict(X):
    global model
    if model:  
        return model.predict(X)
    return None
