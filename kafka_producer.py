from kafka import KafkaProducer
import json
import time
from faker import Faker

# Initialize Faker for generating fake credit card data
fake = Faker()

# Kafka configuration
KAFKA_BROKER = 'kafka:9093'  
TOPIC = 'test-topic'            

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

def generate_fake_data():
    """Generate a fake credit card transaction."""
    return {
        'credit_card_number': fake.credit_card_number(),
        'expiration_date': fake.credit_card_expire(),
        'cvv': fake.random_number(digits=3),
        'amount': round(fake.pyfloat(min_value=10, max_value=500, right_digits=2), 2),
        'merchant': fake.company(),
        'timestamp': time.time()
    }

if __name__ == '__main__':
    print(f"Producing messages to topic '{TOPIC}'...")

    try:
        while True:
            # Generate fake credit card data
            data = generate_fake_data()

            # Send the data to Kafka
            producer.send(TOPIC, value=data)
            print(f"Sent: {data}")

            # Add a small delay between messages
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Close the producer
        producer.close()
