import time
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Histogram
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

transaction_amounts = Histogram(
    'transaction_amounts',
    'Distribution of transaction amounts processed by the consumer',
    ['producer_id']
)

processing_latency = Histogram(
    'transaction_processing_latency_seconds',
    'Time taken to process transactions',
    ['producer_id']
)

errors_encountered = Counter(
    'consumer_errors',
    'Number of errors encountered by the consumer',
    ['producer_id']
)

# Expose metrics on port 8003
start_http_server(8003)

def main():
    for message in consumer:
        transaction = message.value
        producer_id = transaction.get("producer_id", "unknown")
        start_time = time.time()
        try:
            print(f"Consumed from {producer_id}: {transaction}")
            transaction_amounts.labels(producer_id=producer_id).observe(transaction.get("amount", 0))
            transactions_processed.labels(producer_id=producer_id).inc()
            processing_latency.labels(producer_id=producer_id).observe(time.time() - start_time)
        except Exception as e:
            errors_encountered.labels(producer_id=producer_id).inc()
            print(f"Error processing message: {e}")
        time.sleep(1)

if __name__ == '__main__':
    main()
