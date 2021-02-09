"""
Point d'entrée pour l'API Flask.

Exemple de requête : 

import requests
requests.post("http://0.0.0.0:5000/duration/predict", json=
    {
        "trips": [
            {
            "trip_distance": 3.04,
            "PULocationID": 41,
            "DOLocationID": 244,
            "pickup_hour": 14,
            "pickup_minute": 18,
            "pickup_second": 39
            }
        ]
    }
).json()
"""

import pandas as pd

from flask import Flask, request, jsonify
from src.model import load_model, predict

app = Flask(__name__)

load_model()

def require_body_parameters(required_parameters):
    """
    Requiert que le corps de la requête contienne les champs requis.

    Parameters
    ----------
    required_parameters : list
        Les champs requis.
    """
    def decorator(func):
        
        def wrapper(**kwargs):
            body = request.get_json() # On récupère le corps de la requête
            
            required_parameters_set = set(required_parameters)
            fields_set = set(body.keys())
            
            if not required_parameters_set <= fields_set:
                return {'error': "Missing fields."}, 400
            
            return func(**kwargs)
        
        return wrapper
    
    return decorator

@app.route('/duration/predict', methods=['POST'])
@require_body_parameters({'trips'})
def predict_duration():
    body = request.get_json()
    df = pd.DataFrame.from_dict(body['trips'])
    results = {'durations': list(predict(df).flatten())}
    return jsonify(results), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
