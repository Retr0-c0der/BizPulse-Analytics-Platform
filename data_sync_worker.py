# data_sync_worker.py
import os
import sys
import logging
import argparse
from typing import Optional, Dict, Any
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from cryptography.fernet import Fernet, InvalidToken

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# BizPulse Internal DB
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Encryption
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY:
    logger.critical("ENCRYPTION_KEY is not set. Cannot decrypt passwords.")
    sys.exit(1)
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

# --- Helper Functions ---
def decrypt_password(encrypted_password: str) -> Optional[str]:
    try:
        return cipher_suite.decrypt(encrypted_password.encode()).decode()
    except InvalidToken:
        logger.error("Could not decrypt password; token is invalid.")
        return None

def get_internal_db_connection():
    try:
        conn = mysql.connector.connect(host=DB_HOST, database=DB_DATABASE, user=DB_USER, password=DB_PASSWORD)
        return conn
    except Error as e:
        logger.error(f"Failed to connect to internal BizPulse DB: {e}")
        return None

def fetch_connection_details(internal_conn, user_id: int) -> Optional[Dict[str, Any]]:
    cursor = internal_conn.cursor(dictionary=True)
    query = "SELECT * FROM database_connections WHERE user_id = %s AND is_valid = TRUE LIMIT 1"
    cursor.execute(query, (user_id,))
    details = cursor.fetchone()
    cursor.close()
    if isinstance(details, dict):
        return details
    return None

def sync_data_for_user(user_id: int):
    logger.info(f"--- Starting data sync for User ID: {user_id} ---")
    
    internal_conn = get_internal_db_connection()
    if not internal_conn:
        return

    try:
        # 1. Get user's external DB credentials from our DB
        conn_details = fetch_connection_details(internal_conn, user_id)
        if not conn_details:
            logger.warning(f"No valid connection details found for user {user_id}. Skipping.")
            return

        # 2. Decrypt the password
        db_pass = decrypt_password(str(conn_details['encrypted_db_password']))
        if not db_pass:
            return

        # 3. Connect to the user's external database
        logger.info(f"Connecting to external DB for user {user_id} at {conn_details['db_host']}")
        external_conn = mysql.connector.connect(
            host=str(conn_details['db_host']), port=int(conn_details['db_port']),
            user=str(conn_details['db_user']), password=db_pass,
            database=str(conn_details['db_name'])
        )
        logger.info("External DB connection successful.")

        # 4. Fetch data from external DB
        # IMPORTANT: This assumes the user's DB has tables named 'sales' and 'inventory'
        # with specific column names. This is a simplification for now.
        sales_df = pd.read_sql("SELECT product_id, quantity, timestamp FROM external_sales", external_conn)
        inventory_df = pd.read_sql("SELECT product_id, stock_level, last_updated FROM external_inventory", external_conn)
        logger.info(f"Fetched {len(sales_df)} sales records and {len(inventory_df)} inventory records.")
        external_conn.close()

        # 5. Save the data to our internal DB
        cursor = internal_conn.cursor()
        
        # --- Handle Sales Data ---
        if not sales_df.empty:
            sales_df['user_id'] = user_id
            logger.info("Clearing old sales data for user.")
            cursor.execute("DELETE FROM sales_transactions WHERE user_id = %s", (user_id,))
            
            sales_tuples =[tuple(x) for x in sales_df[['user_id', 'product_id', 'quantity', 'timestamp']].to_numpy()]
            cursor.executemany(
                "INSERT INTO sales_transactions (user_id, product_id, quantity, timestamp) VALUES (%s, %s, %s, %s)", 
                sales_tuples
            )

        # --- Handle Inventory Data ---
        if not inventory_df.empty:
            inventory_df['user_id'] = user_id
            logger.info("Resetting and updating inventory data in the products table.")
            
            # Zero out current stock instead of deleting from a view
            cursor.execute("UPDATE products SET current_stock = 0 WHERE user_id = %s", (user_id,))
            
            # Provide a fallback product_name since it's a required field in `products`
            inventory_df['product_name'] = 'Ext Prod ' + inventory_df['product_id'].astype(str)
            inventory_tuples =[tuple(x) for x in inventory_df[['user_id', 'product_id', 'product_name', 'stock_level', 'last_updated']].to_numpy()]
            
            # UPSERT into the products table
            upsert_query = """
                INSERT INTO products (user_id, sku, product_name, current_stock, updated_at) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    current_stock = VALUES(current_stock),
                    updated_at = VALUES(updated_at)
            """
            cursor.executemany(upsert_query, inventory_tuples)
            
        internal_conn.commit()
        logger.info("Successfully synced data into BizPulse DB.")
        cursor.close()

    except Error as e:
        logger.error(f"A database error occurred during sync for user {user_id}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during sync for user {user_id}: {e}", exc_info=True)
    finally:
        if internal_conn and internal_conn.is_connected():
            internal_conn.close()
    
    logger.info(f"--- Finished data sync for User ID: {user_id} ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data sync for a specific user.")
    parser.add_argument("--user-id", type=int, required=True, help="The ID of the user to sync data for.")
    args = parser.parse_args()
    
    sync_data_for_user(args.user_id)