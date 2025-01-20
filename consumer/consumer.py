import time
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter
import json

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'transactions'

# Set up Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id='transaction-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Prometheus metrics
transactions_processed = Counter(
    'transactions_processed',
    'Number of transactions processed by the consumer',
    ['producer_id']
)

# Expose metrics on port 8003
start_http_server(8003)

def main():
    for message in consumer:
        transaction = message.value
        producer_id = transaction.get("producer_id", "unknown")
        print(f"Consumed from {producer_id}: {transaction}")
        transactions_processed.labels(producer_id=producer_id).inc()
        time.sleep(1)

if __name__ == '__main__':
    main()
