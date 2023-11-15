from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "stock_transactions"
OUTPUT_TOPIC = "stock_analysis"

env = StreamExecutionEnvironment.get_execution_environment()

# Kafka Consumer - Reads stock transactions
consumer = FlinkKafkaConsumer(
    topics=INPUT_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties={"bootstrap.servers": KAFKA_BROKER}
)

# Kafka Producer - Sends stock analysis results
producer = FlinkKafkaProducer(
    topic=OUTPUT_TOPIC,
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers": KAFKA_BROKER}
)

# Streaming Processing Function
def process_stock(stream):
    def analyze_stock(data):
        stock = json.loads(data)
        symbol = stock["symbol"]
        price = stock["price"]
        timestamp = stock["timestamp"]

        # Simple moving average logic
        if symbol not in price_history:
            price_history[symbol] = []

        price_history[symbol].append(price)
        if len(price_history[symbol]) > 5:  # Keep last 5 prices
            price_history[symbol].pop(0)

        avg_price = sum(price_history[symbol]) / len(price_history[symbol])

        # Detect sudden price changes (>5% in 1 min)
        change = ((price - avg_price) / avg_price) * 100 if avg_price > 0 else 0
        trend = "Stable"
        if change > 5:
            trend = "Price Surge 🚀"
        elif change < -5:
            trend = "Price Drop 📉"

        return json.dumps({"symbol": symbol, "avg_price": avg_price, "trend": trend, "timestamp": timestamp})

    return stream.map(analyze_stock)

# Apply processing
price_history = {}
stock_stream = env.add_source(consumer)
processed_stream = process_stock(stock_stream)
processed_stream.add_sink(producer)

# Start the Flink Job
env.execute("Stock Trend Analysis with Flink")

