import random
import time
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Histogram
import json
from faker import Faker

fake = Faker()
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'transactions'

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Prometheus metrics
transactions_sent = Counter(
    'transactions_sent',
    'Number of transactions sent by the producer',
    ['producer_id']
)

transaction_amounts = Histogram(
    'transaction_amounts',
    'Distribution of transaction amounts',
    ['producer_id']
)

# Expose metrics on port 8001
start_http_server(8001)

def generate_fake_transaction():
    return {
        "producer_id": "producer1",
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": fake.date_time_this_year().isoformat()
    }

def main():
    try:
        while True:
            transaction = generate_fake_transaction()
            producer.send(TOPIC, value=transaction)
            transactions_sent.labels(producer_id="producer1").inc()
            transaction_amounts.labels(producer_id="producer1").observe(transaction["amount"])
            print(f"Sent: {transaction}")
            time.sleep(random.randint(1, 3))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Close the producer
        producer.close()

if __name__ == '__main__':
    main()
