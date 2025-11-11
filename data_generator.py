# data_generator.py (COMPLETE - ENHANCED FOR DEMO)
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

# Configure a logger for this module
logger = logging.getLogger(__name__)

# --- DEMO ENHANCEMENT: Copied from the live simulator for consistency ---
PRODUCT_IDS = [f"PROD_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]
PRODUCT_WEIGHTS = {product: random.uniform(0.5, 2.0) for product in PRODUCT_IDS}
PRODUCT_COMBOS = {
    'PROD_0001': 'PROD_0002',
    'PROD_0003': 'PROD_0007',
    'PROD_0004': 'PROD_0005',
    'PROD_0008': 'PROD_0001',
}
# --- END OF DEMO ENHANCEMENT ---


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
    for a specific user. NOW WITH PATTERNED "COMBO" DATA.
    """
    logger.info(f"Starting SMARTER historical data generation for user_id: {user_id}")
    producer = None
    try:
        producer = get_kafka_producer()
        total_records = 0
        
        for day in range(HISTORICAL_DAYS, 0, -1):
            daily_records_processed = 0
            # Target a range of sales, not records, since combos create 2 records
            target_daily_sales = random.randint(40, 100) 

            while daily_records_processed < target_daily_sales:
                record_time = datetime.now(timezone.utc) - timedelta(days=day, hours=random.randint(0, 23))
                
                # --- NEW LOGIC START: CREATE COMBOS IN HISTORICAL DATA ---
                
                records_to_send = []
                # Decide if this transaction will be a sale or inventory update
                if random.random() < 0.85:
                    transaction_type = 'SALE'
                    
                    # Decide if this sale should be a combo
                    if random.random() < 0.25: # 25% of sales are combos
                        base_product = random.choice(list(PRODUCT_COMBOS.keys()))
                        combo_product = PRODUCT_COMBOS[base_product]
                        
                        # Create the first record
                        records_to_send.append({
                            'user_id': user_id, 'product_id': base_product,
                            'quantity': random.randint(1, 5), 'transaction_type': transaction_type,
                            'timestamp': record_time.isoformat()
                        })
                        # Create the second, linked record
                        records_to_send.append({
                            'user_id': user_id, 'product_id': combo_product,
                            'quantity': random.randint(1, 5), 'transaction_type': transaction_type,
                            'timestamp': (record_time + timedelta(seconds=30)).isoformat() # Slightly different time
                        })
                    else: # A regular, non-combo sale
                        product_id = random.choices(PRODUCT_IDS, weights=[PRODUCT_WEIGHTS[p] for p in PRODUCT_IDS], k=1)[0]
                        records_to_send.append({
                            'user_id': user_id, 'product_id': product_id,
                            'quantity': random.randint(1, 8), 'transaction_type': transaction_type,
                            'timestamp': record_time.isoformat()
                        })
                else:
                    transaction_type = 'INVENTORY_UPDATE'
                    product_id = random.choices(PRODUCT_IDS, weights=[PRODUCT_WEIGHTS[p] for p in PRODUCT_IDS], k=1)[0]
                    records_to_send.append({
                        'user_id': user_id, 'product_id': product_id,
                        'quantity': random.randint(100, 200), 'transaction_type': transaction_type,
                        'timestamp': record_time.isoformat()
                    })

                # --- NEW LOGIC END ---

                for record in records_to_send:
                    producer.send(KAFKA_TOPIC, value=record)
                    total_records += 1
                
                daily_records_processed += 1
        
        logger.info(f"Successfully queued {total_records} historical records for user_id: {user_id}")

    except Exception as e:
        logger.error(f"Error during data generation for user_id {user_id}: {e}")
    finally:
        if producer:
            producer.flush(timeout=10)
            producer.close()
            logger.info(f"Kafka producer closed for user_id: {user_id}")