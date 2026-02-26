from kafka import KafkaProducer
import requests
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_KEY = 'your_alpha_vantage_key'

while True:
    response = requests.get(f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey={API_KEY}')
    stock_data = response.json()
    producer.send('stock_prices', stock_data)
    print("Sent stock data to Kafka")
    time.sleep(60)  # every minute
