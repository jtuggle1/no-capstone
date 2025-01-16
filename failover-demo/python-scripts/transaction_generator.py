import requests
import json
import random
import time

def generate_transaction():
    return {
        "card_number": f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(1, 100), 2),
        "timestamp": time.time()
    }

while True:
    transaction = generate_transaction()
    try:
        response = requests.post("http://loadbalancer:8080/transaction", json=transaction)
        print(f"Sent: {transaction} | Status: {response.status_code}")
    except Exception as e:
        print(f"Failed to send transaction: {e}")
    time.sleep(1)
