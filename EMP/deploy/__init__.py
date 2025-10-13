import azure.functions as func
import joblib
import json
import pandas as pd

model = joblib.load('employee_churn_model.pkl')

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        df = pd.DataFrame(req_body['data'])
        
        predictions = model.predict(df)
        
        return func.HttpResponse(
            json.dumps({'predictions': predictions.tolist()}),
            mimetype='application/json'
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({'error': str(e)}),
            status_code=400
        )