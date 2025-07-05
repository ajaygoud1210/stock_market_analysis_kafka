# 📈 Real-Time Stock Market Data Pipeline using Kafka, AWS S3, and Athena

## 🚀 Overview

This project demonstrates how to build a real-time data pipeline to simulate stock market streaming using **Apache Kafka**, process it with **Python**, and store the results in **Amazon S3**. The data is later made queryable using **AWS Athena** via **Glue Crawlers** and **Data Catalog**.

✅ It’s designed to showcase core skills in data engineering, including real-time ingestion, cloud data storage, automation, and SQL analytics — all wrapped into an end-to-end project you can showcase.

---

## 🗺️ Architecture

I have uploaded the architecture as jpg. Do check once for clear understanding
---

## 🛠️ Technologies Used

| Stack       | Tools / Services                                      |
|-------------|--------------------------------------------------------|
| Programming | Python 3, kafka-python                                |
| Messaging   | Apache Kafka 3.6.1 (producer/consumer/topic setup)    |
| Cloud       | AWS EC2, S3, Glue Crawler, Glue Catalog, Athena       |
| Infra       | Linux Shell (Amazon Linux 2), ZooKeeper               |
| Storage     | JSON format in S3                                     |
| Querying    | SQL (Athena)                                          |

---

## 📂 Dataset

You can use any tabular stock dataset (CSV format) to simulate real-time market feeds.

In this project, I used a processed sample:
[📊 Stock Market CSV Dataset](https://github.com/ajaygoud1210/stock_market_analysis_kafka/indexProcessed.csv)

---

## 🔁 Data Flow

1. **Kafka Producer (Python)**  
   - Reads CSV rows one by one and pushes them to a Kafka topic (`demo-test`) in real-time.

2. **Kafka Broker (EC2)**  
   - Hosted on AWS EC2, handles topic management and message delivery.

3. **Kafka Consumer (Python)**  
   - Subscribes to `demo-test` topic, receives the stock messages.
   - Each message is written as a `.json` file into a specified S3 bucket (`kafka-project-ajay`).

4. **Glue Crawler & Catalog**  
   - Crawls S3 bucket, creates schema in Glue Data Catalog.

5. **AWS Athena**  
   - Connects to Glue Catalog, queries the S3 data using SQL.

---

## 🧪 Steps to Reproduce

### 1. Setup Kafka on EC2
```bash
sudo yum install java-1.8.0-openjdk
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xvf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1
```

Edit `config/server.properties`:
```
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://<EC2_PUBLIC_IP>:9092
```

Start services:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
bin/kafka-server-start.sh config/server.properties
```

---

### 2. Create Kafka Topic
```bash
bin/kafka-topics.sh --create --topic demo-test --bootstrap-server <EC2_PUBLIC_IP>:9092 --replication-factor 1 --partitions 1
```

---

### 3. Run Producer (Python)
```python
from kafka import KafkaProducer
from json import dumps
import pandas as pd

producer = KafkaProducer(
    bootstrap_servers=['<EC2_PUBLIC_IP>:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

df = pd.read_csv('indexProcessed.csv')
for row in df.to_dict(orient="records"):
    producer.send('demo-test', value=row)
```

---

### 4. Run Consumer (Python)
```python
from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer(
    'demo-test',
    bootstrap_servers=['<EC2_PUBLIC_IP>:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

s3 = S3FileSystem(client_kwargs={'region_name': 'us-east-2'})
for count, message in enumerate(consumer):
    with s3.open(f"s3://kafka-project-ajay/stock_data_{count}.json", 'w') as file:
        json.dump(message.value, file)
```

---

### 5. Set Up Athena

- Configure Athena output to `s3://kafka-project-ajay/athena-results/`
- Create a Glue Crawler for your S3 bucket
- Run the crawler to register schema in Glue Catalog
- Use Athena to run SQL queries like:
```sql
SELECT * FROM stock_market_data
WHERE open > close
LIMIT 10;
```

---

## 📁 Project Structure

```
stock-market-kafka-data-engineering-project/
│
├── Architecture.jpg             # Project diagram
├── KafkaProducer.ipynb         # Sends data to Kafka
├── KafkaConsumer.ipynb         # Stores data to S3
├── indexProcessed.csv          # Sample dataset
├── command_kafka.txt           # All terminal setup commands
└── README.md                   # Project overview and documentation
```

---

## 🔍 What You’ll Learn

How to simulate real-time data streaming using Kafka  
Deploy Kafka brokers and topics on AWS EC2  
Connect Kafka to Python using `kafka-python`  
Store streaming data in S3 and query it via Athena  
Work with AWS Glue Crawlers and Data Catalog  

---

## 🙋 About Me

**Ajay Kumar**  
🎓 MS in Computer Science, University of Dayton  
🔍 Actively seeking Data Engineering roles
🔗 [LinkedIn](https://www.linkedin.com/in/ajayyacharam/)  
📬 [GitHub](https://github.com/ajaygoud1210)

---

## 🏷️ Tags

`#ApacheKafka` `#AWS` `#RealTimeStreaming` `#DataEngineering` `#Athena` `#Glue` `#Python`

