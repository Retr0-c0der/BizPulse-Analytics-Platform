# sales_data_simulator.py (COMPLETE - Merged Version)

import json
import random
import time
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import mysql.connector
from mysql.connector import Error
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
DB_HOST = 'localhost'
DB_DATABASE = os.getenv('DB_DATABASE', 'bizpulse_db')
DB_USER = os.getenv('DB_USER', 'bizpulse_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'userpassword')
NUM_PRODUCTS = int(os.getenv('NUM_PRODUCTS', 10))
SIMULATION_INTERVAL = float(os.getenv('SIMULATION_INTERVAL_SECONDS', 2.0))
GENERATE_HISTORICAL = os.getenv('GENERATE_HISTORICAL', 'true').lower() == 'true'
HISTORICAL_DAYS = int(os.getenv('HISTORICAL_DAYS', 30))

# Generate product IDs
PRODUCT_IDS = [f"PROD_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]

# Product popularity weights
PRODUCT_WEIGHTS = {product: random.uniform(0.5, 2.0) for product in PRODUCT_IDS}


class SalesDataSimulator:
    """Simulates realistic sales and inventory update events with optional historical data."""
    
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
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    compression_type='gzip'
                )
                logger.info(f"✅ Kafka Producer connected to {KAFKA_BROKER}")
                return producer
                
            except Exception as e:
                retry_count += 1
                logger.error(
                    f"Failed to connect to Kafka (attempt {retry_count}/{max_retries}): {e}"
                )
                if retry_count < max_retries:
                    time.sleep(5 * retry_count)
                else:
                    logger.critical("Max retries reached. Exiting.")
                    raise
    
    def get_db_connection(self) -> Optional[mysql.connector.connection.MySQLConnection]:
        """Creates and returns a MySQL database connection."""
        try:
            connection = mysql.connector.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD,
                connection_timeout=10
            )
            if connection.is_connected():
                return connection
        except Error as e:
            logger.warning(f"Database connection failed: {e}")
            return None
    
    # --- NEW, CORRECTED FUNCTION ---
    def get_in_stock_products(self) -> List[str]:
        """
        Fetches products currently in stock from the database.
        Falls back to all products if DB unavailable.
        """
        db_conn = self.get_db_connection()
        
        if not db_conn:
            logger.warning("⚠️  DB unavailable, using all products for sale generation")
            return PRODUCT_IDS
        
        try:
            cursor = db_conn.cursor()
            # CORRECTED QUERY: Use the 'inventory' table and 'stock_level' column
            query = "SELECT product_id FROM inventory WHERE stock_level > 0 AND user_id = 1"
            cursor.execute(query)
            in_stock = [row[0] for row in cursor.fetchall()]
            
            if not in_stock:
                logger.warning("⚠️  No products in stock, sales will be paused until next restock.")
                # Return an empty list to prevent sales
                return []
                
            logger.debug(f"Found {len(in_stock)} products in stock")
            return in_stock
            
        except Error as e:
            logger.error(f"Error querying inventory: {e}")
            # In case of error, fall back to all products to prevent crashing
            return PRODUCT_IDS
        finally:
            if db_conn and db_conn.is_connected():
                cursor.close()
                db_conn.close()
    
    def get_time_based_multiplier(self, target_time: datetime) -> float:
        """Returns a multiplier based on time of day."""
        hour = target_time.hour
        
        if 10 <= hour <= 20:  # Peak hours
            return random.uniform(1.5, 2.5)
        elif hour < 6 or hour > 23:  # Late night
            return random.uniform(0.3, 0.8)
        else:  # Moderate hours
            return random.uniform(0.8, 1.5)
    
    def get_day_based_multiplier(self, target_time: datetime) -> float:
        """Returns a multiplier based on day of week."""
        day_of_week = target_time.weekday()
        
        if day_of_week >= 4:  # Weekend
            return random.uniform(1.3, 1.8)
        else:  # Weekday
            return random.uniform(0.9, 1.2)
    
    def generate_historical_record(self, days_ago: int) -> Dict:
        """Generates a record for a specific time in the past."""
        # Create timestamp for the past
        record_time = datetime.now(timezone.utc) - timedelta(
            days=days_ago,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Select product (weighted by popularity)
        product_id = random.choices(
            PRODUCT_IDS,
            weights=[PRODUCT_WEIGHTS[p] for p in PRODUCT_IDS],
            k=1
        )[0]
        
        # Apply time patterns
        time_mult = self.get_time_based_multiplier(record_time)
        day_mult = self.get_day_based_multiplier(record_time)
        overall_mult = time_mult * day_mult
        
        # Determine transaction type (higher sale probability)
        if random.random() < 0.85:
            transaction_type = 'SALE'
            base_quantity = random.randint(1, 8)
            quantity = max(1, int(base_quantity * overall_mult))
        else:
            transaction_type = 'INVENTORY_UPDATE'
            quantity = random.randint(100, 200)
        
        return {
            'user_id': 1,
            'product_id': product_id,
            'quantity': quantity,
            'transaction_type': transaction_type,
            'timestamp': record_time.isoformat()
        }
    
    def generate_live_record(self, in_stock_products: List[str]) -> Dict:
        """Generates a live record, respecting current inventory."""
        # Use in-stock products for sales, all products for restocks
        use_in_stock = random.random() < 0.85  # 85% chance of sale
        
        if use_in_stock and in_stock_products:
            product_pool = in_stock_products
        else:
            product_pool = PRODUCT_IDS
        
        # Select product (weighted by popularity if available)
        if product_pool == PRODUCT_IDS:
            product_id = random.choices(
                product_pool,
                weights=[PRODUCT_WEIGHTS[p] for p in product_pool],
                k=1
            )[0]
        else:
            product_id = random.choice(product_pool)
        
        # Current time
        now = datetime.now(timezone.utc)
        
        # Apply time patterns
        time_mult = self.get_time_based_multiplier(now)
        day_mult = self.get_day_based_multiplier(now)
        overall_mult = time_mult * day_mult
        
        # Determine transaction
        sale_probability = min(0.95, 0.75 + (overall_mult - 1) * 0.1)
        
        if random.random() < sale_probability and in_stock_products:
            transaction_type = 'SALE'
            # Ensure we only sell from in-stock products
            if product_id not in in_stock_products:
                product_id = random.choice(in_stock_products)
            base_quantity = random.randint(1, 5)
            quantity = max(1, int(base_quantity * overall_mult))
        else:
            transaction_type = 'INVENTORY_UPDATE'
            quantity = random.randint(50, 150)
        
        return {
            'user_id': 1,
            'product_id': product_id,
            'quantity': quantity,
            'transaction_type': transaction_type,
            'timestamp': now.isoformat()
        }
    
    def send_record(self, record: Dict) -> bool:
        """Sends a record to Kafka with error handling."""
        try:
            future = self.producer.send(KAFKA_TOPIC, value=record)
            future.get(timeout=10)
            
            self.records_sent += 1
            return True
            
        except Exception as e:
            self.errors += 1
            logger.error(f"Failed to send record: {e}")
            return False
    
    def generate_historical_data(self):
        """Generates and sends historical data for the past N days."""
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 PHASE 1: Generating {HISTORICAL_DAYS} days of historical data")
        logger.info(f"{'='*60}\n")
        
        total_records = 0
        
        for day in range(HISTORICAL_DAYS, 0, -1):
            # Generate variable number of transactions per day
            daily_records = random.randint(80, 200)
            
            for _ in range(daily_records):
                record = self.generate_historical_record(days_ago=day)
                self.send_record(record)
                total_records += 1
            
            if day % 5 == 0:  # Progress update every 5 days
                logger.info(f"✓ Generated data up to {day} days ago ({total_records} records)")
            
            # Small delay to avoid overwhelming Kafka
            if day % 10 == 0:
                self.producer.flush()
                time.sleep(0.5)
        
        # Final flush
        self.producer.flush()
        
        logger.info(f"\n✅ Historical data complete: {total_records} records generated")
        logger.info(f"⏳ Waiting 10 seconds for stream processor to catch up...\n")
        time.sleep(10)
    
    def run_live_simulation(self):
        """Run live, inventory-aware simulation."""
        logger.info(f"\n{'='*60}")
        logger.info(f"🔴 PHASE 2: Starting LIVE inventory-aware simulation")
        logger.info(f"{'='*60}")
        logger.info(f"   Topic: {KAFKA_TOPIC}")
        logger.info(f"   Interval: {SIMULATION_INTERVAL}s")
        logger.info(f"   Products: {len(PRODUCT_IDS)}")
        logger.info(f"   Press Ctrl+C to stop.\n")
        
        try:
            while True:
                # Refresh in-stock products periodically
                in_stock = self.get_in_stock_products()
                
                # Generate and send live record
                record = self.generate_live_record(in_stock)
                
                if self.send_record(record):
                    logger.info(
                        f"[{self.records_sent:04d}] {record['transaction_type']:17} | "
                        f"{record['product_id']} x {record['quantity']:3d} | "
                        f"In-stock: {len(in_stock)}/{len(PRODUCT_IDS)}"
                    )
                
                # Periodic status
                if self.records_sent % 50 == 0:
                    logger.info(
                        f"\n📊 Status: {self.records_sent} sent, {self.errors} errors\n"
                    )
                
                time.sleep(SIMULATION_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Simulation stopped by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
    
    def run(self):
        """Main entry point."""
        self.producer = self.get_kafka_producer()
        
        try:
            # Phase 1: Historical data (optional)
            if GENERATE_HISTORICAL:
                self.generate_historical_data()
            
            # Phase 2: Live simulation
            self.run_live_simulation()
            
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown."""
        if self.producer:
            logger.info("\n📤 Flushing remaining messages...")
            self.producer.flush(timeout=10)
            self.producer.close()
            logger.info(
                f"✅ Shutdown complete. "
                f"Total: {self.records_sent} sent, {self.errors} errors"
            )


if __name__ == "__main__":
    simulator = SalesDataSimulator()
    simulator.run()