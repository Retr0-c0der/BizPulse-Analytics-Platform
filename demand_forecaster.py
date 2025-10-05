# demand_forecaster.py
import os
import pandas as pd
from prophet import Prophet
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Database Configuration ---
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def get_db_connection():
    """Establishes and returns a connection to the MySQL database."""
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
        print(f"🔥 Error connecting to MySQL Database: {e}")
        return None

def train_and_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trains a Prophet model for each product and forecasts demand for the next 30 days.

    Args:
        df: A DataFrame with historical sales data containing 'product_id', 'timestamp', and 'quantity'.

    Returns:
        A DataFrame with the 30-day forecast for all products.
    """
    all_forecasts = []
    
    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    for product in df['product_id'].unique():
        print(f"Forecasting for product: {product}...")
        
        # Prepare data for Prophet: requires 'ds' and 'y' columns
        product_df = df[df['product_id'] == product][['timestamp', 'quantity']]
        product_df.rename(columns={'timestamp': 'ds', 'quantity': 'y'}, inplace=True)
        
        # Initialize and fit the model
        model = Prophet(daily_seasonality=True)
        model.fit(product_df)
        
        # Create future dataframe and predict
        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        
        # Filter for future dates and select relevant columns
        future_forecast = forecast[forecast['ds'] > product_df['ds'].max()]
        future_forecast = future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        
        # Add product_id back and format for DB insertion
        future_forecast['product_id'] = product
        future_forecast.rename(columns={
            'ds': 'forecast_date',
            'yhat': 'predicted_units',
            'yhat_lower': 'confidence_low',
            'yhat_upper': 'confidence_high'
        }, inplace=True)
        
        # Round predictions to nearest integer
        future_forecast['predicted_units'] = future_forecast['predicted_units'].round().astype(int)
        
        all_forecasts.append(future_forecast)
        
    # --- THIS IS THE FIX ---
    # Combine all forecasts into a single DataFrame
    final_forecasts = pd.concat(all_forecasts, ignore_index=True)
    
    # Re-order columns to match the database schema exactly before returning
    column_order = ['product_id', 'forecast_date', 'predicted_units', 'confidence_low', 'confidence_high']
    return final_forecasts[column_order]

def update_forecast_db(forecast_data: pd.DataFrame):
    """
    Clears the existing forecast table and inserts the new predictions.
    
    Args:
        forecast_data: DataFrame containing the new forecast predictions.
    """
    conn = get_db_connection()
    if not conn:
        return
        
    cursor = conn.cursor()
    try:
        print("Clearing old forecast data...")
        cursor.execute("TRUNCATE TABLE forecast_predictions;")
        
        print(f"Inserting {len(forecast_data)} new forecast records...")
        insert_query = """
        INSERT INTO forecast_predictions 
        (product_id, forecast_date, predicted_units, confidence_low, confidence_high) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Convert dataframe to list of tuples for insertion
        data_tuples = [tuple(x) for x in forecast_data.to_numpy()]
        cursor.executemany(insert_query, data_tuples)
        
        conn.commit()
        print("✅ Forecast data successfully updated in the database.")
        
    except Error as e:
        print(f"🔥 Error during database update: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 1. Fetch historical sales data from the database
    db_conn = get_db_connection()
    if db_conn:
        # For this example, we assume the 'sales_transactions' table is populated
        # A stream processor would be responsible for this in a real system
        query = "SELECT product_id, timestamp, quantity FROM sales_transactions"
        historical_df = pd.read_sql(query, db_conn)
        db_conn.close()

        if not historical_df.empty:
            # 2. Train model and generate forecast
            forecast_results = train_and_forecast(historical_df)
            
            # 3. Update the database with new forecasts
            update_forecast_db(forecast_results)
        else:
            print("⚠️ No historical sales data found in 'sales_transactions' table to generate a forecast.")

#
# --- HOW TO CONTAINERIZE AND SCHEDULE THIS SCRIPT ---
#
# 1. Dockerfile: Create a `Dockerfile` for this script.
#    FROM python:3.9-slim
#    WORKDIR /app
#    COPY requirements.txt .
#    RUN pip install --no-cache-dir -r requirements.txt
#    COPY demand_forecaster.py .
#    CMD ["python", "demand_forecaster.py"]
#
#    (requirements.txt would include pandas, prophet, mysql-connector-python)
#
# 2. Scheduling: Run this container on a schedule.
#    - Local/Simple: Use a host machine's cron job to run `docker run --network=bizpulse-net forecaster-image`.
#    - Cloud-Native (AWS): Use AWS Lambda with an EventBridge (CloudWatch Events) trigger to run daily.
#    - Cloud-Native (Kubernetes): Use a Kubernetes CronJob resource to run the container on a schedule.
#