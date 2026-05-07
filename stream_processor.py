# stream_processor.py (TENANT-AWARE)
import os
import json
import time
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = 'bizpulse_sales_stream'
DB_HOST = os.getenv('DB_HOST', 'mysql_db')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

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
    user_id = data.get('user_id')
    product_id = data.get('product_id')
    quantity = data.get('quantity')
    transaction_type = data.get('transaction_type')
    timestamp = data.get('timestamp')

    if not all([user_id, product_id, quantity, transaction_type, timestamp]):
        print(f"🔥 Invalid message received, skipping: {data}")
        return

    cursor = None
    try:
        cursor = db_conn.cursor()

        if transaction_type == 'SALE':
            # 1. Log the sale transaction
            cursor.execute("""
                INSERT INTO sales_transactions (user_id, product_id, quantity, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (user_id, product_id, quantity, timestamp))

            # 2. Update inventory table
            cursor.execute("""
                UPDATE inventory 
                SET stock_level = GREATEST(0, stock_level - %s), last_updated = %s
                WHERE user_id = %s AND product_id = %s
            """, (quantity, timestamp, user_id, product_id))

            # 3. Sync products table so it stays in step with inventory
            cursor.execute("""
                UPDATE products
                SET current_stock = GREATEST(0, current_stock - %s)
                WHERE user_id = %s AND sku = %s
            """, (quantity, user_id, product_id))

            # 4. Add to Stock Movements Audit Log (negative quantity for sales)
            cursor.execute("""
                INSERT INTO stock_movements (user_id, sku, product_name, movement_type, quantity, note, performed_by, created_at)
                SELECT %s, %s, product_name, 'out', %s, 'Automated Sale (Simulator)', 'System', %s
                FROM products WHERE user_id = %s AND sku = %s
            """, (user_id, product_id, -quantity, timestamp, user_id, product_id))

            print(f"Processed SALE for User {user_id}: Product {product_id}, Qty: -{quantity}")

        elif transaction_type == 'INVENTORY_UPDATE':
            # 1. Update inventory table
            cursor.execute("""
                UPDATE inventory 
                SET stock_level = stock_level + %s, last_updated = %s
                WHERE user_id = %s AND product_id = %s
            """, (quantity, timestamp, user_id, product_id))

            # 2. Sync products table
            cursor.execute("""
                UPDATE products
                SET current_stock = current_stock + %s
                WHERE user_id = %s AND sku = %s
            """, (quantity, user_id, product_id))

            # 3. Add to Stock Movements Audit Log
            cursor.execute("""
                INSERT INTO stock_movements (user_id, sku, product_name, movement_type, quantity, note, performed_by, created_at)
                SELECT %s, %s, product_name, 'in', %s, 'Automated Restock (Simulator)', 'System', %s
                FROM products WHERE user_id = %s AND sku = %s
            """, (user_id, product_id, quantity, timestamp, user_id, product_id))

            print(f"Processed INVENTORY_UPDATE for User {user_id}: Product {product_id}, Qty: +{quantity}")

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

    print("🚀 Stream processor running (Tenant-Aware Mode)... waiting for messages.")

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
