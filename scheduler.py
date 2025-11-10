# scheduler.py (IMPROVED LOGGING VERSION)
import time
import subprocess
import logging
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'), database=os.getenv('DB_DATABASE'),
            user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Error as e:
        logger.error(f"Scheduler DB connection failed: {e}")
        return None

def get_users_with_valid_connections():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT user_id FROM database_connections WHERE is_valid = TRUE;")
        user_ids = [item[0] for item in cursor.fetchall()]
        if user_ids:
            logger.info(f"Found {len(user_ids)} users with valid connections to sync.")
        return user_ids
    finally:
        if conn and conn.is_connected():
            conn.close()

def run_sync_for_users(user_ids):
    if not user_ids:
        logger.info("SCHEDULER: No users to sync. Skipping data sync.")
        return
        
    logger.info(f"SCHEDULER: Starting data sync for users: {user_ids}")
    for user_id in user_ids:
        try:
            logger.info(f"--- Triggering sync for user_id: {user_id} ---")
            result = subprocess.run(
                ["python", "data_sync_worker.py", "--user-id", str(user_id)],
                check=True, capture_output=True, text=True, timeout=60
            )
            # --- ALWAYS LOG OUTPUT ---
            logger.info(f"--- SYNC WORKER STDOUT (User {user_id}) ---\n{result.stdout}")
            if result.stderr:
                logger.warning(f"--- SYNC WORKER STDERR (User {user_id}) ---\n{result.stderr}")
            logger.info(f"--- Sync completed for user_id: {user_id} ---")

        except subprocess.CalledProcessError as e:
            logger.error(f"🔥 Sync worker FAILED for user_id {user_id}! Return Code: {e.returncode} 🔥")
            logger.error(f"--- FAILED STDOUT ---\n{e.stdout}")
            logger.error(f"--- FAILED STDERR ---\n{e.stderr}")
        except subprocess.TimeoutExpired as e:
            logger.error(f"🔥 Sync worker TIMED OUT for user_id {user_id}! 🔥")

def run_forecaster():
    logger.info("SCHEDULER: Triggering the demand_forecaster.py script for all users...")
    try:
        result = subprocess.run(
            ["python", "demand_forecaster.py"],
            check=True, capture_output=True, text=True, timeout=120
        )
        logger.info(f"--- FORECASTER STDOUT ---\n{result.stdout}")
        logger.info("SCHEDULER: demand_forecaster.py finished successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"🔥 SCHEDULER: demand_forecaster.py FAILED! 🔥\n--- STDERR ---\n{e.stderr}")

def run_analytics():
    logger.info("SCHEDULER: Triggering the analytics_worker.py script...")
    try:
        result = subprocess.run(
            ["python", "analytics_worker.py"],
            check=True, capture_output=True, text=True, timeout=180
        )
        logger.info(f"--- ANALYTICS WORKER STDOUT ---\n{result.stdout}")
        logger.info("SCHEDULER: analytics_worker.py finished successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"🔥 SCHEDULER: analytics_worker.py FAILED! 🔥\n--- STDERR ---\n{e.stderr}")

if __name__ == "__main__":
    logger.info("🚀 Scheduler Service Started. Sync, Forecast & Analytics will run every 60 seconds.")
    
    cycle_count = 0
    while True:
        try:
            # Phase 1: Sync data
            users_to_sync = get_users_with_valid_connections()
            run_sync_for_users(users_to_sync)
            
            # Phase 2: Run forecasting
            run_forecaster()
            
            # Phase 3: Run analytics (e.g., every 5th cycle to reduce load)
            if cycle_count % 5 == 0:
                run_analytics()

        except Exception as e:
            logger.error(f"An unexpected error occurred in the scheduler loop: {e}", exc_info=True)

        cycle_count += 1
        logger.info(f"SCHEDULER: Cycle {cycle_count} complete. Waiting for 60 seconds...")
        time.sleep(60)