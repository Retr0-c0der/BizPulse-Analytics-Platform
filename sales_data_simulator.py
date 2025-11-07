# sales_data_simulator.py
import json
import random
import time
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'bizpulse_sales_stream')
NUM_PRODUCTS = int(os.getenv('NUM_PRODUCTS', 10))
SIMULATION_INTERVAL = float(os.getenv('SIMULATION_INTERVAL_SECONDS', 2.0))

# Generate product IDs
PRODUCT_IDS = [f"PROD_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]

# Product popularity weights (some products sell more than others)
PRODUCT_WEIGHTS = {
    product: random.uniform(0.5, 2.0) 
    for product in PRODUCT_IDS
}

class SalesDataSimulator:
    """Simulates realistic sales and inventory update events."""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.records_sent = 0
        self.errors = 0
        
    def get_kafka_producer(self) -> KafkaProducer:
        """Initializes and returns a Kafka producer with retry logic."""
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3,   # Retry on transient failures
                    max_in_flight_requests_per_connection=1,  # Maintain order
                    compression_type='gzip'  # Compress messages
                )
                logger.info("✅ Kafka Producer connected successfully")
                return producer
                
            except Exception as e:
                retry_count += 1
                logger.error(
                    f"Failed to connect to Kafka at {KAFKA_BROKER} "
                    f"(attempt {retry_count}/{max_retries}): {e}"
                )
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)  # Exponential backoff
                else:
                    logger.critical("Max retries reached. Exiting.")
                    raise
    
    def get_time_based_multiplier(self) -> float:
        """
        Returns a multiplier based on time of day to simulate realistic patterns.
        Higher sales during business hours, lower at night.
        """
        current_hour = datetime.now().hour
        
        # Peak hours: 10 AM - 8 PM
        if 10 <= current_hour <= 20:
            return random.uniform(1.5, 2.5)
        # Early morning/late night: lower activity
        elif current_hour < 6 or current_hour > 23:
            return random.uniform(0.3, 0.8)
        # Other times: moderate activity
        else:
            return random.uniform(0.8, 1.5)
    
    def get_day_based_multiplier(self) -> float:
        """
        Returns a multiplier based on day of week.
        Higher sales on weekends.
        """
        day_of_week = datetime.now().weekday()
        
        # Weekend (Friday-Sunday): higher sales
        if day_of_week >= 4:
            return random.uniform(1.3, 1.8)
        # Weekday: normal sales
        else:
            return random.uniform(0.9, 1.2)
    
    def generate_record(self) -> Dict:
        """Generates a realistic sales or inventory record with patterns."""
        # Select product (weighted by popularity)
        product_id = random.choices(
            PRODUCT_IDS,
            weights=[PRODUCT_WEIGHTS[p] for p in PRODUCT_IDS],
            k=1
        )[0]
        
        # Apply time-based and day-based patterns
        time_multiplier = self.get_time_based_multiplier()
        day_multiplier = self.get_day_based_multiplier()
        overall_multiplier = time_multiplier * day_multiplier
        
        # Determine transaction type
        # Higher sale probability during peak times
        sale_probability = min(0.95, 0.75 + (overall_multiplier - 1) * 0.1)
        
        if random.random() < sale_probability:
            transaction_type = 'SALE'
            # Quantity influenced by time patterns
            base_quantity = random.randint(1, 5)
            quantity = max(1, int(base_quantity * overall_multiplier))
        else:
            transaction_type = 'INVENTORY_UPDATE'
            quantity = random.randint(50, 150)
        
        # Use UTC timestamp with timezone info
        record = {
            'product_id': product_id,
            'quantity': quantity,
            'transaction_type': transaction_type,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        return record
    
    def send_record(self, record: Dict) -> bool:
        """
        Sends a record to Kafka with error handling.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            future = self.producer.send(KAFKA_TOPIC, value=record)
            
            # Wait for confirmation (with timeout)
            record_metadata = future.get(timeout=10)
            
            self.records_sent += 1
            logger.info(
                f"Sent [{self.records_sent}] {record['transaction_type']}: "
                f"{record['product_id']} x{record['quantity']}"
            )
            return True
            
        except KafkaError as e:
            self.errors += 1
            logger.error(f"Failed to send record: {e}")
            return False
        except Exception as e:
            self.errors += 1
            logger.error(f"Unexpected error sending record: {e}")
            return False
    
    def run(self):
        """Main simulation loop."""
        self.producer = self.get_kafka_producer()
        
        logger.info(
            f"🚀 Starting data simulation...\n"
            f"   Topic: {KAFKA_TOPIC}\n"
            f"   Interval: {SIMULATION_INTERVAL}s\n"
            f"   Products: {len(PRODUCT_IDS)}\n"
            f"   Press Ctrl+C to stop."
        )
        
        try:
            while True:
                record = self.generate_record()
                self.send_record(record)
                
                # Periodic status update
                if self.records_sent % 100 == 0:
                    logger.info(
                        f"📊 Status: {self.records_sent} sent, "
                        f"{self.errors} errors"
                    )
                
                time.sleep(SIMULATION_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Simulation stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in simulation: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the producer."""
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush(timeout=10)
            self.producer.close()
            logger.info(
                f"✅ Kafka Producer closed. "
                f"Total: {self.records_sent} sent, {self.errors} errors"
            )

if __name__ == "__main__":
    simulator = SalesDataSimulator()
    simulator.run()