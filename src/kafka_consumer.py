from kafka import KafkaConsumer
import json
from collections import deque
import numpy as np

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Change if using a different broker
TOPIC_NAME = "stock_transactions"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Store last N prices for trend analysis
price_history = {}  # {symbol: deque([price1, price2, ...])}
WINDOW_SIZE = 5  # Number of latest prices to consider for trend

def detect_trend(symbol, new_price):
    """Analyze stock trends based on price history."""
    if symbol not in price_history:
        price_history[symbol] = deque(maxlen=WINDOW_SIZE)

    price_history[symbol].append(new_price)
    
    if len(price_history[symbol]) < WINDOW_SIZE:
        return "Not enough data"

    # Calculate trend direction
    prices = np.array(price_history[symbol])
    trend = np.polyfit(range(WINDOW_SIZE), prices, 1)[0]  # Linear regression slope

    if trend > 0:
        return "Uptrend 📈"
    elif trend < 0:
        return "Downtrend 📉"
    return "Stable"

print("Starting Kafka Consumer... Listening for stock tra

