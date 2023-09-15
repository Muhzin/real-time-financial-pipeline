from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  # Change if using a different broker
TOPIC_NAME = "stock_transactions"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# List of sample stock symbols
stocks = ["AAPL", "GOOG", "TSLA", "MSFT", "AMZN"]

def generate_stock_data():
    """Generate a random stock transaction."""
    return {
        "symbol": random.choice(stocks),
        "price": round(random.uniform(100, 1500), 2),
        "volume": random.randint(10, 1000),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting Kafka Producer...")
    while True:
        stock_data = generate_stock_data()
        producer.send(TOPIC_NAME, stock_data)
        print(f"Produced: {stock_data}")
        time.sleep(1)  # Simulate 1 transaction per second

