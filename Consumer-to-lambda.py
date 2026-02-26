import requests
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'stock_prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    payload = json.dumps(message.value)
    requests.post("https://api-id.execute-api.region.amazonaws.com/prod/flatten", data=payload)
