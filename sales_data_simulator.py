# sales_data_simulator.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = 'localhost:9093'  # Note: 9093 is exposed to the host
KAFKA_TOPIC = 'bizpulse_sales_stream'
PRODUCT_IDS = [f"PROD_{str(i).zfill(4)}" for i in range(1, 11)] # e.g., PROD_0001
SIMULATION_INTERVAL_SECONDS = 2 # Generate data every 2 seconds

# --- Kafka Producer Setup ---
def get_kafka_producer():
    """Initializes and returns a Kafka producer, handling connection errors."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            print(f"🔥 Failed to connect to Kafka Broker at {KAFKA_BROKER}. Retrying in 5 seconds...")
            print(f"Error: {e}")
            time.sleep(5)

def generate_record():
    """Generates a single realistic sales or inventory record."""
    product_id = random.choice(PRODUCT_IDS)
    
    # 80% chance of a SALE, 20% chance of an INVENTORY_UPDATE
    if random.random() < 0.8:
        transaction_type = 'SALE'
        quantity = random.randint(1, 5) # Small quantity for a sale
    else:
        transaction_type = 'INVENTORY_UPDATE'
        quantity = random.randint(50, 100) # Larger quantity for restocking
        
    record = {
        'product_id': product_id,
        'quantity': quantity,
        'transaction_type': transaction_type,
        'timestamp': datetime.now().isoformat()
    }
    return record

# --- Main Simulation Loop ---
if __name__ == "__main__":
    producer = get_kafka_producer()
    print(f"🚀 Starting data simulation... Pushing to topic '{KAFKA_TOPIC}'. Press Ctrl+C to stop.")
    
    try:
        while True:
            record = generate_record()
            producer.send(KAFKA_TOPIC, value=record)
            print(f"Sent record: {record}")
            
            # Flush to ensure messages are sent, especially in low-throughput scenarios
            producer.flush() 
            
            time.sleep(SIMULATION_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped by user.")
    finally:
        if producer:
            producer.close()
            print("Kafka Producer closed.")