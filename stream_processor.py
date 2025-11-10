# stream_processor.py (TENANT-AWARE)
import os
import json
import time
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'bizpulse_sales_stream'
DB_HOST = os.getenv('DB_HOST', 'mysql_db')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
# --- NEW: Define the default user for all incoming data ---
# DEFAULT_USER_ID = 1 

def get_kafka_consumer(topic):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='bizpulse-processor-group'
            )
            print("✅ Kafka Consumer connected successfully.")
            return consumer
        except Exception as e:
            print(f"🔥 Failed to connect to Kafka Broker at {KAFKA_BROKER}. Retrying in 5 seconds...")
            print(f"Error: {e}")
            time.sleep(5)

def get_db_connection():
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
    # --- NEW: Get user_id from the message ---
    user_id = data.get('user_id')
    product_id = data.get('product_id')
    quantity = data.get('quantity')
    transaction_type = data.get('transaction_type')
    timestamp = data.get('timestamp')

    # Basic validation
    if not all([user_id, product_id, quantity, transaction_type, timestamp]):
        print(f"🔥 Invalid message received, skipping: {data}")
        return

    cursor = None
    try:
        cursor = db_conn.cursor()
        
        if transaction_type == 'SALE':
            insert_sale_query = """
            INSERT INTO sales_transactions (user_id, product_id, quantity, timestamp)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(insert_sale_query, (user_id, product_id, quantity, timestamp))
            
            update_inventory_query = """
            INSERT INTO inventory (user_id, product_id, stock_level, last_updated)
            VALUES (%s, %s, -%s, %s)
            ON DUPLICATE KEY UPDATE stock_level = stock_level - %s, last_updated = %s;
            """
            cursor.execute(update_inventory_query, (user_id, product_id, quantity, timestamp, quantity, timestamp))
            
            print(f"Processed SALE for User {user_id}: Product {product_id}, Quantity: {quantity}")

        elif transaction_type == 'INVENTORY_UPDATE':
            update_inventory_query = """
            INSERT INTO inventory (user_id, product_id, stock_level, last_updated)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE stock_level = stock_level + %s, last_updated = %s;
            """
            cursor.execute(update_inventory_query, (user_id, product_id, quantity, timestamp, quantity, timestamp))
            
            print(f"Processed INVENTORY_UPDATE for User {user_id}: Product {product_id}, Quantity: {quantity}")
            
        db_conn.commit()
        
    except Error as e:
        print(f"🔥 Database Error: {e}")
        if db_conn:
            db_conn.rollback()
    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    consumer = get_kafka_consumer(KAFKA_TOPIC)
    db_connection = get_db_connection()
    
    print("🚀 Stream processor is running (Tenant-Aware Mode)... waiting for messages.")
    
    try:
        for message in consumer:
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