# data_generator.py
import json
import random
import time
import logging
import os
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# Basic configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'bizpulse_sales_stream'
HISTORICAL_DAYS = 30
NUM_PRODUCTS = 10

# Note: This is now stateless, so we regenerate these on each call.
PRODUCT_IDS = [f"PROD_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]
PRODUCT_WEIGHTS = {product: random.uniform(0.5, 2.0) for product in PRODUCT_IDS}

# Configure a logger for this module
logger = logging.getLogger(__name__)

def get_kafka_producer() -> KafkaProducer:
    """Initializes and returns a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka for data generation: {e}")
        raise

def generate_historical_data_for_user(user_id: int):
    """
    Generates a batch of historical sales/inventory data and sends it to Kafka
    for a specific user.
    """
    logger.info(f"Starting historical data generation for user_id: {user_id}")
    producer = None
    try:
        producer = get_kafka_producer()
        total_records = 0
        
        for day in range(HISTORICAL_DAYS, 0, -1):
            daily_records = random.randint(80, 200)
            for _ in range(daily_records):
                # This logic is copied from sales_data_simulator.py
                record_time = datetime.now(timezone.utc) - timedelta(days=day, hours=random.randint(0, 23))
                product_id = random.choices(PRODUCT_IDS, weights=[PRODUCT_WEIGHTS[p] for p in PRODUCT_IDS], k=1)[0]
                
                if random.random() < 0.85:
                    transaction_type = 'SALE'
                    quantity = random.randint(1, 8)
                else:
                    transaction_type = 'INVENTORY_UPDATE'
                    quantity = random.randint(100, 200)

                record = {
                    'user_id': user_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'transaction_type': transaction_type,
                    'timestamp': record_time.isoformat()
                }
                
                producer.send(KAFKA_TOPIC, value=record)
                total_records += 1
        
        logger.info(f"Successfully queued {total_records} historical records for user_id: {user_id}")

    except Exception as e:
        logger.error(f"Error during data generation for user_id {user_id}: {e}")
    finally:
        if producer:
            producer.flush(timeout=10)
            producer.close()
            logger.info(f"Kafka producer closed for user_id: {user_id}")