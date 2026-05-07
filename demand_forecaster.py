# demand_forecaster.py (TENANT-AWARE)
import os
import logging
import sys
from typing import Optional, List, Any, cast
import pandas as pd
from prophet import Prophet
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
FORECAST_DAYS = int(os.getenv('FORECAST_DAYS', 30))
MIN_HISTORICAL_DAYS = int(os.getenv('MIN_HISTORICAL_DAYS', 14))
MIN_TRANSACTIONS = int(os.getenv('MIN_TRANSACTIONS', 10))

def get_db_connection() -> Optional[Any]:
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, database=DB_DATABASE,
            user=DB_USER, password=DB_PASSWORD
        )
        if conn.is_connected():
            return conn
    except Error as e:
        logger.error(f"Error connecting to MySQL Database: {e}")
        return None

def get_all_user_ids(conn: mysql.connector.MySQLConnection) -> List[int]:
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users WHERE is_active = TRUE;")
        user_ids = [cast(int, item[0]) for item in cursor.fetchall()]
        logger.info(f"Found {len(user_ids)} active users to process.")
        return user_ids
    except Error as e:
        logger.error(f"Could not fetch user IDs: {e}")
        return []
    finally:
        cursor.close()

def calculate_and_save_stockout_alerts(conn: mysql.connector.MySQLConnection, user_id: int):
    """
    Generates two types of alerts:
    1. PREDICTED_STOCK_OUT  – from forecast vs current inventory level
    2. LOW_STOCK            – current_stock < min_stock_level in products table
    """
    logger.info(f"User {user_id}: Starting alert calculation...")
    cursor = conn.cursor()
    try:
        # ── Part A: Predicted stock-out alerts (existing logic, unchanged) ──
        inventory_df = pd.read_sql(
            "SELECT product_id, stock_level FROM inventory WHERE user_id = %s",
            conn, params=(user_id,)
        )
        forecast_df = pd.read_sql(
            """SELECT product_id, forecast_date, predicted_units
               FROM forecast_predictions WHERE user_id = %s ORDER BY forecast_date""",
            conn, params=(user_id,)
        )

        predicted_alerts = []
        today = pd.Timestamp.now().normalize()

        if not inventory_df.empty and not forecast_df.empty:
            for _, row in inventory_df.iterrows():
                product_id = row['product_id']
                current_stock = row['stock_level']
                product_forecast = forecast_df[forecast_df['product_id'] == product_id].copy()
                if product_forecast.empty:
                    continue
                product_forecast['cumulative_demand'] = product_forecast['predicted_units'].cumsum()
                stockout_day = product_forecast[product_forecast['cumulative_demand'] >= current_stock]
                if not stockout_day.empty:
                    first_stockout_date = pd.to_datetime(stockout_day.iloc[0]['forecast_date'])
                    days_until_stockout = (first_stockout_date - today).days
                    if days_until_stockout >= 0:
                        message = f"Predicted to stock out in {days_until_stockout} days."
                        predicted_alerts.append((user_id, product_id, 'PREDICTED_STOCK_OUT', message, 'critical'))

        # ── Part B: Low-stock alerts from products table ──
        # Only runs if the products table exists and has rows for this user.
        low_stock_alerts = []
        try:
            cursor.execute("""
                SELECT sku, product_name, current_stock, min_stock_level
                FROM products
                WHERE user_id = %s AND current_stock < min_stock_level AND current_stock >= 0
            """, (user_id,))
            low_stock_rows = cursor.fetchall()
            for row in low_stock_rows:
                sku = row[0]
                product_name = row[1]
                current_stock = cast(int, row[2])
                min_stock_level = cast(int, row[3])
                shortage = min_stock_level - current_stock
                severity = 'critical' if current_stock == 0 else 'warning'
                if current_stock == 0:
                    message = f"{product_name} is OUT OF STOCK (min: {min_stock_level} units)."
                else:
                    message = f"{product_name} is low: {current_stock} units left, {shortage} below minimum."
                low_stock_alerts.append((user_id, sku, 'LOW_STOCK', message, severity))
        except Error:
            # products table may not exist yet for older deployments — skip gracefully
            logger.warning(f"User {user_id}: Could not query products table for low-stock alerts (table may not exist yet).")

        # ── Commit: clear old alerts, insert fresh ones ──
        logger.info(f"User {user_id}: Clearing old alerts...")
        cursor.execute("""
            DELETE FROM product_alerts
            WHERE user_id = %s AND alert_type IN ('PREDICTED_STOCK_OUT', 'LOW_STOCK')
        """, (user_id,))

        all_alerts = predicted_alerts + low_stock_alerts
        if all_alerts:
            cursor.executemany("""
                INSERT INTO product_alerts
                    (user_id, product_id, alert_type, alert_message, severity)
                VALUES (%s, %s, %s, %s, %s)
            """, all_alerts)
            logger.info(f"User {user_id}: Inserted {len(all_alerts)} alerts "
                        f"({len(predicted_alerts)} predicted stock-out, {len(low_stock_alerts)} low-stock).")
        else:
            logger.info(f"User {user_id}: No alerts to insert — all stock levels healthy.")

        conn.commit()

    except Exception as e:
        logger.error(f"User {user_id}: Alert calculation failed: {e}", exc_info=True)
        conn.rollback()
    finally:
        cursor.close()

def fetch_user_sales_data(conn: mysql.connector.MySQLConnection, user_id: int) -> pd.DataFrame:
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

            date_range = (pd.to_datetime(product_df['ds'].max()) - pd.to_datetime(product_df['ds'].min())).days
            if len(product_df) < MIN_TRANSACTIONS or date_range < MIN_HISTORICAL_DAYS:

                import logging as prophet_logging
                prophet_logging.getLogger('prophet').setLevel(prophet_logging.WARNING)

            model = Prophet(daily_seasonality='auto', weekly_seasonality='auto', yearly_seasonality='auto')  # type: ignore[call-arg]
            model.fit(product_df)
            future = model.make_future_dataframe(periods=FORECAST_DAYS)
            forecast = model.predict(future)

            future_forecast = forecast[forecast['ds'] > product_df['ds'].max()].copy()
            future_forecast['user_id'] = user_id
            future_forecast['product_id'] = product
            future_forecast.rename(columns={
                'ds': 'forecast_date',
                'yhat': 'predicted_units',
                'yhat_lower': 'confidence_low',
                'yhat_upper': 'confidence_high'
            }, inplace=True)

            future_forecast['predicted_units'] = future_forecast['predicted_units'].clip(lower=0).round().astype(int)
            future_forecast['confidence_low'] = future_forecast['confidence_low'].clip(lower=0)
            future_forecast['confidence_high'] = future_forecast['confidence_high'].clip(lower=0)

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
    return pd.concat(all_forecasts, ignore_index=True)

def update_forecast_db(conn: mysql.connector.MySQLConnection, forecast_data: pd.DataFrame, user_id: int) -> bool:
    if forecast_data.empty:
        logger.warning(f"User {user_id}: No forecast data to update.")
        return True

    cursor = conn.cursor()
    try:
        logger.info(f"User {user_id}: Clearing old forecast data...")
        cursor.execute("DELETE FROM forecast_predictions WHERE user_id = %s;", (user_id,))

        logger.info(f"User {user_id}: Inserting {len(forecast_data)} new forecast records...")
        cursor.executemany("""
            INSERT INTO forecast_predictions
                (user_id, product_id, forecast_date, predicted_units, confidence_low, confidence_high)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [tuple(x) for x in forecast_data.to_numpy()])
        conn.commit()
        logger.info(f"User {user_id}: Forecast data successfully updated.")
        return True
    except Error as e:
        logger.error(f"User {user_id}: Error during forecast DB update: {e}")
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
                logger.warning(f"User {user_id}: No sales data found. Skipping forecast.")
                # Still run alert check — products may be manually added with low stock
                calculate_and_save_stockout_alerts(db_conn, user_id)
                continue

            forecast_results = train_and_forecast(historical_df, user_id)
            update_forecast_db(db_conn, forecast_results, user_id)
            calculate_and_save_stockout_alerts(db_conn, user_id)

            logger.info(f"--- Finished forecast process for User ID: {user_id} ---")

    except Exception as e:
        logger.error(f"Fatal error in main execution loop: {e}", exc_info=True)
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()

if __name__ == "__main__":
    main()
