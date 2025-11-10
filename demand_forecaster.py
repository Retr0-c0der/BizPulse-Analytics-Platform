# demand_forecaster.py (TENANT-AWARE)
import os
import logging
import sys
from typing import Optional, List
import pandas as pd
from prophet import Prophet
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# --- Configuration ---
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
FORECAST_DAYS = int(os.getenv('FORECAST_DAYS', 30))
MIN_HISTORICAL_DAYS = int(os.getenv('MIN_HISTORICAL_DAYS', 14))
MIN_TRANSACTIONS = int(os.getenv('MIN_TRANSACTIONS', 10))

def get_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    try:
        conn = mysql.connector.connect(host=DB_HOST, database=DB_DATABASE, user=DB_USER, password=DB_PASSWORD)
        if conn.is_connected():
            return conn
    except Error as e:
        logger.error(f"Error connecting to MySQL Database: {e}")
        return None

def get_all_user_ids(conn: mysql.connector.MySQLConnection) -> List[int]:
    """Fetches a list of all active user IDs."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users WHERE is_active = TRUE;")
        user_ids = [item[0] for item in cursor.fetchall()]
        logger.info(f"Found {len(user_ids)} active users to process.")
        return user_ids
    except Error as e:
        logger.error(f"Could not fetch user IDs: {e}")
        return []
    finally:
        cursor.close()

def calculate_and_save_stockout_alerts(conn: mysql.connector.MySQLConnection, user_id: int):
    logger.info(f"User {user_id}: Starting stock-out alert calculation...")
    try:
        # 1. Fetch current inventory and forecasts for the user
        inventory_query = "SELECT product_id, stock_level FROM inventory WHERE user_id = %s"
        inventory_df = pd.read_sql(inventory_query, conn, params=(user_id,))
        if inventory_df.empty:
            logger.info(f"User {user_id}: No inventory found, skipping alert calculation.")
            return

        forecast_query = """
            SELECT product_id, forecast_date, predicted_units 
            FROM forecast_predictions WHERE user_id = %s ORDER BY forecast_date
        """
        forecast_df = pd.read_sql(forecast_query, conn, params=(user_id,))
        if forecast_df.empty:
            logger.info(f"User {user_id}: No forecast data found, skipping alert calculation.")
            return

        alerts_to_insert = []
        today = pd.Timestamp.now().normalize()

        for _, row in inventory_df.iterrows():
            product_id = row['product_id']
            current_stock = row['stock_level']
            
            product_forecast = forecast_df[forecast_df['product_id'] == product_id].copy()
            if product_forecast.empty:
                continue

            product_forecast['cumulative_demand'] = product_forecast['predicted_units'].cumsum()
            
            # Find the first day where cumulative demand exceeds current stock
            stockout_day = product_forecast[product_forecast['cumulative_demand'] >= current_stock]
            
            if not stockout_day.empty:
                first_stockout_date = pd.to_datetime(stockout_day.iloc[0]['forecast_date'])
                days_until_stockout = (first_stockout_date - today).days
                
                if days_until_stockout >= 0:
                    message = f"Predicted to stock out in {days_until_stockout} days."
                    alerts_to_insert.append((
                        user_id, product_id, 'PREDICTED_STOCK_OUT', message, 'critical'
                    ))

        # 2. Update the database (clear old alerts, insert new ones)
        if alerts_to_insert:
            cursor = conn.cursor()
            logger.info(f"User {user_id}: Clearing old stock-out alerts...")
            cursor.execute("""
                DELETE FROM product_alerts 
                WHERE user_id = %s AND alert_type = 'PREDICTED_STOCK_OUT'
            """, (user_id,))
            
            logger.info(f"User {user_id}: Inserting {len(alerts_to_insert)} new stock-out alerts.")
            insert_query = """
            INSERT INTO product_alerts 
            (user_id, product_id, alert_type, alert_message, severity)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, alerts_to_insert)
            conn.commit()
            cursor.close()

    except Exception as e:
        logger.error(f"User {user_id}: Failed during stock-out alert calculation: {e}", exc_info=True)
        conn.rollback()

def fetch_user_sales_data(conn: mysql.connector.MySQLConnection, user_id: int) -> pd.DataFrame:
    """Fetches all historical sales data for a specific user."""
    query = "SELECT product_id, timestamp, quantity FROM sales_transactions WHERE user_id = %s"
    try:
        df = pd.read_sql(query, conn, params=(user_id,))
        logger.info(f"User {user_id}: Loaded {len(df)} historical sales records.")
        return df
    except Exception as e:
        logger.error(f"User {user_id}: Failed to fetch sales data: {e}")
        return pd.DataFrame()

def train_and_forecast(df: pd.DataFrame, user_id: int) -> pd.DataFrame:
    all_forecasts = []
    if df.empty:
        return pd.DataFrame()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    products = df['product_id'].unique()
    
    for product in products:
        try:
            product_df = df[df['product_id'] == product][['timestamp', 'quantity']].copy()
            product_df.rename(columns={'timestamp': 'ds', 'quantity': 'y'}, inplace=True)
            product_df = product_df.groupby('ds', as_index=False).agg({'y': 'sum'}).sort_values('ds')
            
            if len(product_df) < MIN_TRANSACTIONS or (product_df['ds'].max() - product_df['ds'].min()).days < MIN_HISTORICAL_DAYS:
                logger.warning(f"User {user_id}, Product {product}: Insufficient data. Skipping.")
                continue

            # Suppress Prophet's verbose output
            import logging as prophet_logging
            prophet_logging.getLogger('prophet').setLevel(prophet_logging.WARNING)

            model = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality='auto')
            model.fit(product_df)
            future = model.make_future_dataframe(periods=FORECAST_DAYS)
            forecast = model.predict(future)

            future_forecast = forecast[forecast['ds'] > product_df['ds'].max()].copy()
            future_forecast['user_id'] = user_id
            future_forecast['product_id'] = product
            
            # --- THIS IS THE MAIN FIX ---
            future_forecast.rename(columns={
                'ds': 'forecast_date',
                'yhat': 'predicted_units',
                'yhat_lower': 'confidence_low',
                'yhat_upper': 'confidence_high'
            }, inplace=True)
            
            future_forecast['predicted_units'] = future_forecast['predicted_units'].clip(lower=0).round().astype(int)
            future_forecast['confidence_low'] = future_forecast['confidence_low'].clip(lower=0)
            future_forecast['confidence_high'] = future_forecast['confidence_high'].clip(lower=0)
            
            # Select and reorder columns to match DB schema
            future_forecast = future_forecast[[
                'user_id', 'product_id', 'forecast_date', 'predicted_units', 
                'confidence_low', 'confidence_high'
            ]]
            
            all_forecasts.append(future_forecast)
        except Exception as e:
            logger.error(f"User {user_id}, Product {product}: Error during forecasting: {e}", exc_info=True)
            continue
            
    if not all_forecasts:
        return pd.DataFrame()
    
    final_forecasts = pd.concat(all_forecasts, ignore_index=True)
    return final_forecasts

def update_forecast_db(conn: mysql.connector.MySQLConnection, forecast_data: pd.DataFrame, user_id: int) -> bool:
    if forecast_data.empty:
        logger.warning(f"User {user_id}: No forecast data to update in database.")
        return True # Not a failure, just nothing to do
    
    cursor = conn.cursor()
    try:
        # CRITICAL CHANGE: Delete only the specific user's old data
        logger.info(f"User {user_id}: Clearing old forecast data...")
        cursor.execute("DELETE FROM forecast_predictions WHERE user_id = %s;", (user_id,))
        
        logger.info(f"User {user_id}: Inserting {len(forecast_data)} new forecast records...")
        insert_query = """
        INSERT INTO forecast_predictions 
        (user_id, product_id, forecast_date, predicted_units, confidence_low, confidence_high) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data_tuples = [tuple(x) for x in forecast_data.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
        logger.info(f"User {user_id}: Forecast data successfully updated.")
        return True
    except Error as e:
        logger.error(f"User {user_id}: Error during database update: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def main():
    db_conn = get_db_connection()
    if not db_conn:
        sys.exit("Cannot proceed without a database connection.")

    try:
        user_ids = get_all_user_ids(db_conn)
        if not user_ids:
            logger.warning("No active users found. Exiting.")
            return

        for user_id in user_ids:
            logger.info(f"--- Starting forecast process for User ID: {user_id} ---")
            historical_df = fetch_user_sales_data(db_conn, user_id)
            if historical_df.empty:
                logger.warning(f"User {user_id}: No sales data found. Skipping.")
                continue
            
            forecast_results = train_and_forecast(historical_df, user_id)
            update_forecast_db(db_conn, forecast_results, user_id)
            
            # --- ADD THIS CALL ---
            calculate_and_save_stockout_alerts(db_conn, user_id)
            
            logger.info(f"--- Finished forecast process for User ID: {user_id} ---")

    except Exception as e:
        logger.error(f"Fatal error in main execution loop: {e}", exc_info=True)
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()

if __name__ == "__main__":
    main()