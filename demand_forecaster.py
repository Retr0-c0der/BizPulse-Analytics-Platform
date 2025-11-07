# demand_forecaster.py
import os
import logging
from typing import Optional
import pandas as pd
from prophet import Prophet
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
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Forecasting parameters
FORECAST_DAYS = int(os.getenv('FORECAST_DAYS', 30))
MIN_HISTORICAL_DAYS = int(os.getenv('MIN_HISTORICAL_DAYS', 14))
MIN_TRANSACTIONS = int(os.getenv('MIN_TRANSACTIONS', 10))

def get_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Establishes and returns a connection to the MySQL database."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if conn.is_connected():
            logger.info("MySQL Database connection successful")
            return conn
    except Error as e:
        logger.error(f"Error connecting to MySQL Database: {e}")
        return None

def validate_product_data(product_df: pd.DataFrame, product_id: str) -> bool:
    """
    Validates if product has sufficient data for forecasting.
    
    Args:
        product_df: DataFrame with product sales history
        product_id: Product identifier for logging
        
    Returns:
        True if data is sufficient, False otherwise
    """
    if len(product_df) < MIN_TRANSACTIONS:
        logger.warning(
            f"Product {product_id}: Insufficient data points "
            f"({len(product_df)} < {MIN_TRANSACTIONS}). Skipping."
        )
        return False
    
    date_range = (product_df['ds'].max() - product_df['ds'].min()).days
    if date_range < MIN_HISTORICAL_DAYS:
        logger.warning(
            f"Product {product_id}: Insufficient date range "
            f"({date_range} days < {MIN_HISTORICAL_DAYS} days). Skipping."
        )
        return False
    
    return True

def prepare_product_data(df: pd.DataFrame, product_id: str) -> pd.DataFrame:
    """
    Prepares and aggregates product data for Prophet.
    
    Args:
        df: Raw sales DataFrame
        product_id: Product to prepare data for
        
    Returns:
        Prepared DataFrame with daily aggregated sales
    """
    product_df = df[df['product_id'] == product_id][['timestamp', 'quantity']].copy()
    product_df.rename(columns={'timestamp': 'ds', 'quantity': 'y'}, inplace=True)
    
    # Aggregate multiple transactions per day
    product_df = product_df.groupby('ds', as_index=False).agg({'y': 'sum'})
    
    # Sort by date to ensure proper time series
    product_df = product_df.sort_values('ds').reset_index(drop=True)
    
    return product_df

def train_and_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trains a Prophet model for each product and forecasts demand.

    Args:
        df: DataFrame with historical sales data containing 
            'product_id', 'timestamp', and 'quantity'.

    Returns:
        DataFrame with forecasts for all products with sufficient data.
    """
    all_forecasts = []
    
    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    products = df['product_id'].unique()
    logger.info(f"Starting forecast for {len(products)} products")
    
    for product in products:
        try:
            logger.info(f"Processing product: {product}")
            
            # Prepare and validate data
            product_df = prepare_product_data(df, product)
            
            if not validate_product_data(product_df, product):
                continue
            
            # Initialize and fit the model with error handling
            model = Prophet(
                daily_seasonality=True,
                weekly_seasonality=True,
                yearly_seasonality='auto',
                interval_width=0.95  # 95% confidence interval
            )
            
            # Suppress Prophet's verbose output
            import logging as prophet_logging
            prophet_logging.getLogger('prophet').setLevel(prophet_logging.WARNING)
            
            model.fit(product_df)
            
            # Create future dataframe and predict
            future = model.make_future_dataframe(periods=FORECAST_DAYS)
            forecast = model.predict(future)
            
            # Filter for future dates only
            future_forecast = forecast[forecast['ds'] > product_df['ds'].max()].copy()
            future_forecast = future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            
            # Add product_id and format columns
            future_forecast['product_id'] = product
            future_forecast.rename(columns={
                'ds': 'forecast_date',
                'yhat': 'predicted_units',
                'yhat_lower': 'confidence_low',
                'yhat_upper': 'confidence_high'
            }, inplace=True)
            
            # Ensure non-negative predictions (can't have negative sales)
            future_forecast['predicted_units'] = future_forecast['predicted_units'].clip(lower=0)
            future_forecast['confidence_low'] = future_forecast['confidence_low'].clip(lower=0)
            future_forecast['confidence_high'] = future_forecast['confidence_high'].clip(lower=0)
            
            # Round predictions to nearest integer
            future_forecast['predicted_units'] = future_forecast['predicted_units'].round().astype(int)
            
            all_forecasts.append(future_forecast)
            logger.info(f"Successfully forecasted {len(future_forecast)} days for {product}")
            
        except Exception as e:
            logger.error(f"Error forecasting product {product}: {e}", exc_info=True)
            continue
    
    if not all_forecasts:
        logger.warning("No products had sufficient data for forecasting")
        return pd.DataFrame()
    
    # Combine all forecasts
    final_forecasts = pd.concat(all_forecasts, ignore_index=True)
    
    # Re-order columns to match database schema
    column_order = ['product_id', 'forecast_date', 'predicted_units', 
                    'confidence_low', 'confidence_high']
    
    logger.info(f"Generated {len(final_forecasts)} total forecast records")
    return final_forecasts[column_order]

def update_forecast_db(forecast_data: pd.DataFrame) -> bool:
    """
    Clears the existing forecast table and inserts new predictions.
    
    Args:
        forecast_data: DataFrame containing the new forecast predictions.
        
    Returns:
        True if successful, False otherwise
    """
    if forecast_data.empty:
        logger.warning("No forecast data to update in database")
        return False
    
    conn = get_db_connection()
    if not conn:
        return False
        
    cursor = conn.cursor()
    try:
        logger.info("Clearing old forecast data...")
        cursor.execute("TRUNCATE TABLE forecast_predictions;")
        
        logger.info(f"Inserting {len(forecast_data)} new forecast records...")
        insert_query = """
        INSERT INTO forecast_predictions 
        (product_id, forecast_date, predicted_units, confidence_low, confidence_high) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Convert dataframe to list of tuples
        data_tuples = [tuple(x) for x in forecast_data.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        
        conn.commit()
        logger.info("Forecast data successfully updated in database")
        return True
        
    except Error as e:
        logger.error(f"Error during database update: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def main():
    """Main execution function."""
    try:
        # Fetch historical sales data
        db_conn = get_db_connection()
        if not db_conn:
            logger.error("Cannot proceed without database connection")
            return
        
        query = "SELECT product_id, timestamp, quantity FROM sales_transactions"
        logger.info("Fetching historical sales data...")
        historical_df = pd.read_sql(query, db_conn)
        db_conn.close()

        if historical_df.empty:
            logger.warning("No historical sales data found in 'sales_transactions' table")
            return

        logger.info(f"Loaded {len(historical_df)} historical sales records")
        
        # Train model and generate forecast
        forecast_results = train_and_forecast(historical_df)
        
        # Update the database with new forecasts
        if not forecast_results.empty:
            update_forecast_db(forecast_results)
        else:
            logger.warning("No forecasts generated to save to database")
            
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()
