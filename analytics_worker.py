# analytics_worker.py
import os
import sys
import logging
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_db_connection():
    try:
        conn = mysql.connector.connect(host=DB_HOST, database=DB_DATABASE, user=DB_USER, password=DB_PASSWORD)
        return conn
    except Error as e:
        logger.error(f"Failed to connect to DB: {e}")
        return None

def analyze_and_store_rules_for_user(conn, user_id):
    logger.info(f"--- Starting Market Basket Analysis for User ID: {user_id} ---")
    try:
        # 1. Fetch sales data
        query = """
            SELECT product_id, timestamp 
            FROM sales_transactions 
            WHERE user_id = %s AND timestamp >= NOW() - INTERVAL 90 DAY
        """
        df = pd.read_sql(query, conn, params=(user_id,), parse_dates=['timestamp'])
        
        if len(df) < 50:
            logger.warning(f"User {user_id}: Insufficient sales data for analysis ({len(df)} records). Skipping.")
            return

        # 2. Create transaction baskets
        # We'll group sales within a 5-minute window as a single "basket"
        df = df.sort_values('timestamp').set_index('timestamp')
        baskets = df.groupby(pd.Grouper(freq='5Min'))['product_id'].apply(list)
        transactions = [basket for basket in baskets if len(basket) > 1]

        if not transactions:
            logger.warning(f"User {user_id}: No multi-item transactions found. Skipping.")
            return

        # 3. One-hot encode the data for the Apriori algorithm
        te = TransactionEncoder()
        te_ary = te.fit(transactions).transform(transactions)
        df_encoded = pd.DataFrame(te_ary, columns=te.columns_)

        # 4. Run Apriori and generate association rules
        frequent_itemsets = apriori(df_encoded, min_support=0.001, use_colnames=True)
        if frequent_itemsets.empty:
            logger.warning(f"User {user_id}: No frequent itemsets found with current support level.")
            return
            
        rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1) # Lowered min_threshold
        
        # DEMO CHANGE: Lowered confidence for more results
        # Filter for high-confidence, single-item rules
        rules = rules[(rules['confidence'] > 0.2) & (rules['antecedents'].apply(lambda x: len(x) == 1)) & (rules['consequents'].apply(lambda x: len(x) == 1))]
        if rules.empty:
            logger.info(f"User {user_id}: No high-confidence association rules found.")
            return

        # 5. Format and store results
        rules['antecedents'] = rules['antecedents'].apply(lambda x: list(x)[0])
        rules['consequents'] = rules['consequents'].apply(lambda x: list(x)[0])
        rules_to_store = rules[['antecedents', 'consequents', 'confidence', 'lift']].head(10) # Store top 10
        
        rules_tuples = []
        for _, row in rules_to_store.iterrows():
            rules_tuples.append((
                user_id, row['antecedents'], row['consequents'], row['confidence'], row['lift']
            ))

        cursor = conn.cursor()
        logger.info(f"User {user_id}: Clearing old association rules.")
        cursor.execute("DELETE FROM association_rules WHERE user_id = %s", (user_id,))

        logger.info(f"User {user_id}: Storing {len(rules_tuples)} new association rules.")
        insert_query = "INSERT INTO association_rules (user_id, antecedents, consequents, confidence, lift) VALUES (%s, %s, %s, %s, %s)"
        cursor.executemany(insert_query, rules_tuples)
        conn.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"An error occurred during analysis for user {user_id}: {e}", exc_info=True)
        conn.rollback()

def main():
    conn = get_db_connection()
    if not conn:
        sys.exit("Cannot proceed without a database connection.")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users WHERE is_active = TRUE;")
        user_ids = [item[0] for item in cursor.fetchall()]
        cursor.close()
        
        for user_id in user_ids:
            analyze_and_store_rules_for_user(conn, user_id)
            
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    main()