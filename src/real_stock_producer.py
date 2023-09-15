import yfinance as yf
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"  # Change if using a different broker
TOPIC_NAME = "stock_transactions"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# List of stock symbols to track
stocks = ["AAPL", "GOOG", "TSLA", "MSFT", "AMZN"]

def fetch_stock_price(symbol):
    """Fetch real-time stock price using Yahoo Finance."""
    try:
        stock = yf.Ticker(symbol)
        price = stock.history(period="1m")["Close"].iloc[-1]
        return round(price, 2)
    except Exception as e:
        print(f"Error fetching stock data for {symbol}: {e}")
        return None

if __name__ == "__main__":
    print("Starting Kafka Producer with Real Stock Data...")
    
    while True:
        for symbol in stocks:
            price = fetch_stock_price(symbol)
            if price:
                stock_data = {
                    "symbol": symbol,
                    "price": price,
                    "volume": 100,  # Static volume for simplicity
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(TOPIC_NAME, stock_data)
                print(f"Produced: {stock_data}")
            time.sleep(2)  # Simulate API calls with a small delay

