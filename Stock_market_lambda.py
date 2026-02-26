import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'stock-data-cleaned'

def flatten_quote(quote):
    return {
        "symbol": quote["01. symbol"],
        "open": float(quote["02. open"]),
        "high": float(quote["03. high"]),
        "price": float(quote.get("05. price", 0)),
        "volume": int(quote.get("06. volume", 0)),
        "timestamp": datetime.utcnow().isoformat()
    }

def lambda_handler(event, context):
    body = json.loads(event['body'])
    flat_data = flatten_quote(body["Global Quote"])
    
    key = f"flattened/{flat_data['symbol']}/{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(flat_data)
    )
    
    return {
        'statusCode': 200,
        'body': 'Data Flattened and Stored Successfully'
    }
