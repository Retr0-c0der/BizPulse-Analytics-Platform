# stream_processor.py
import os
import json
import time
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092') # Use internal Docker network name
KAFKA_TOPIC = 'bizpulse_sales_stream'
DB_HOST = os.getenv('DB_HOST', 'mysql_db') # Use internal Docker network name
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_kafka_consumer(topic):
    """Initializes and returns a Kafka consumer, handling connection retries."""
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest', # Start reading from the beginning of the topic
                group_id='bizpulse-processor-group' # Consumer group ID
            )
            print("✅ Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"🔥 Failed to connect to Kafka Broker at {KAFKA_BROKER}. Retrying in 5 seconds...")
            print(f"Error: {e}")
            time.sleep(5)

def get_db_connection():
    """Establishes and returns a connection to the MySQL database, handling retries."""
    while True:
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            if conn.is_connected():
                print("✅ MySQL Database connection successful.")
                return conn
        except Error as e:
            print(f"🔥 Error connecting to MySQL Database at {DB_HOST}. Retrying in 5 seconds...")
            print(f"Error: {e}")
            time.sleep(5)

def process_message(message, db_conn):
    """Processes a single message from Kafka and updates the database."""
    data = message.value
    product_id = data.get('product_id')
    quantity = data.get('quantity')
    transaction_type = data.get('transaction_type')
    timestamp = data.get('timestamp')

    cursor = None
    try:
        cursor = db_conn.cursor()
        
        if transaction_type == 'SALE':
            # 1. Insert the sale into the historical transactions table
            insert_sale_query = """
            INSERT INTO sales_transactions (product_id, quantity, timestamp)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_sale_query, (product_id, quantity, timestamp))
            
            # 2. Update the inventory level (decrement)
            # This query will insert a new product with negative stock if it doesn't exist,
            # which can be a useful way to flag data integrity issues. Or it just decrements.
            update_inventory_query = """
            INSERT INTO inventory (product_id, stock_level, last_updated)
            VALUES (%s, -%s, %s)
            ON DUPLICATE KEY UPDATE stock_level = stock_level - %s, last_updated = %s;
            """
            cursor.execute(update_inventory_query, (product_id, quantity, timestamp, quantity, timestamp))
            
            print(f"Processed SALE: Product {product_id}, Quantity: {quantity}")

        elif transaction_type == 'INVENTORY_UPDATE':
            # Update the inventory level (increment)
            # This robust query inserts the product if it's the first time we've seen it,
            # or updates the stock level if it already exists.
            update_inventory_query = """
            INSERT INTO inventory (product_id, stock_level, last_updated)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE stock_level = stock_level + %s, last_updated = %s;
            """
            cursor.execute(update_inventory_query, (product_id, quantity, timestamp, quantity, timestamp))
            
            print(f"Processed INVENTORY_UPDATE: Product {product_id}, Quantity: {quantity}")
            
        # Commit the transaction to make the changes permanent
        db_conn.commit()
        
    except Error as e:
        print(f"🔥 Database Error: {e}")
        if db_conn:
            db_conn.rollback() # Roll back changes if any part of the transaction fails
    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    consumer = get_kafka_consumer(KAFKA_TOPIC)
    db_connection = get_db_connection()
    
    print("🚀 Stream processor is running... waiting for messages. Press Ctrl+C to stop.")
    
    try:
        for message in consumer:
            # Check if the database connection is still alive, reconnect if not
            if not db_connection.is_connected():
                print("Database connection lost. Reconnecting...")
                db_connection.close()
                db_connection = get_db_connection()
            
            process_message(message, db_connection)
    except KeyboardInterrupt:
        print("\n🛑 Stream processor stopped by user.")
    finally:
        if consumer:
            consumer.close()
        if db_connection and db_connection.is_connected():
            db_connection.close()
        print("Kafka consumer and database connection closed.")