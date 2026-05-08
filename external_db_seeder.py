# external_db_seeder.py
import random
import time
from datetime import datetime, timedelta, timezone
import mysql.connector
from mysql.connector import Error

# Connection details for your mapped external database
DB_HOST = 'localhost' # Use 'external-db' if running inside another Docker container
DB_PORT = 3307
DB_DATABASE = 'external_biz_db'
DB_USER = 'ext_user'
DB_PASSWORD = 'ext_password'

NUM_PRODUCTS = 15
PRODUCT_IDS =[f"EXT_{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]
HISTORICAL_DAYS = 30

def get_time_based_multiplier(target_time: datetime) -> float:
    hour = target_time.hour
    if 10 <= hour <= 20: return random.uniform(1.5, 2.5)
    elif hour < 6 or hour > 23: return random.uniform(0.3, 0.8)
    else: return random.uniform(0.8, 1.5)

def get_day_based_multiplier(target_time: datetime) -> float:
    day_of_week = target_time.weekday()
    if day_of_week >= 4: return random.uniform(1.3, 1.8)
    else: return random.uniform(0.9, 1.2)

def seed_external_database():
    print(f"🔌 Connecting to External DB at {DB_HOST}:{DB_PORT}...")
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, database=DB_DATABASE,
            user=DB_USER, password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # 1. Create tables
        print("🛠️ Creating external tables...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS external_inventory (
                product_id VARCHAR(50) PRIMARY KEY,
                stock_level INT,
                last_updated DATETIME
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS external_sales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id VARCHAR(50),
                quantity INT,
                timestamp DATETIME
            )
        """)

        cursor.execute("TRUNCATE TABLE external_sales")
        cursor.execute("TRUNCATE TABLE external_inventory")

        # 2. Seed Inventory
        print(f"📦 Seeding {NUM_PRODUCTS} products into inventory...")
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        inventory_data =[(pid, random.randint(200, 500), now) for pid in PRODUCT_IDS] # Start with high stock
        cursor.executemany(
            "INSERT INTO external_inventory (product_id, stock_level, last_updated) VALUES (%s, %s, %s)",
            inventory_data
        )

        # 3. Seed Historical Sales
        print(f"📊 PHASE 1: Generating {HISTORICAL_DAYS} days of historical sales data...")
        sales_data = []
        product_weights =[random.uniform(0.5, 2.0) for _ in PRODUCT_IDS]

        for day in range(HISTORICAL_DAYS, 0, -1):
            daily_records = random.randint(80, 200)
            for _ in range(daily_records):
                record_time = datetime.now(timezone.utc) - timedelta(days=day, hours=random.randint(0, 23))
                product_id = random.choices(PRODUCT_IDS, weights=product_weights, k=1)[0]
                
                time_mult = get_time_based_multiplier(record_time)
                day_mult = get_day_based_multiplier(record_time)
                overall_mult = time_mult * day_mult
                
                base_quantity = random.randint(1, 8)
                quantity = max(1, int(base_quantity * overall_mult))
                
                sales_data.append((product_id, quantity, record_time.strftime('%Y-%m-%d %H:%M:%S')))

        cursor.executemany(
            "INSERT INTO external_sales (product_id, quantity, timestamp) VALUES (%s, %s, %s)",
            sales_data
        )
        conn.commit()
        print(f"✅ Historical Data Complete! ({len(sales_data)} records)")

        # 4. LIVE SIMULATION (Streaming mode)
        print("\n" + "="*50)
        print("🟢 PHASE 2: Starting LIVE POS Simulation...")
        print("The external DB is now registering live sales. Press Ctrl+C to stop.")
        print("="*50 + "\n")
        
        while True:
            # Pick a random product to sell
            product_id = random.choices(PRODUCT_IDS, weights=product_weights, k=1)[0]
            quantity = random.randint(1, 5)
            live_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert the sale
            cursor.execute(
                "INSERT INTO external_sales (product_id, quantity, timestamp) VALUES (%s, %s, %s)",
                (product_id, quantity, live_time)
            )
            
            # Deduct from inventory
            cursor.execute(
                "UPDATE external_inventory SET stock_level = GREATEST(0, stock_level - %s), last_updated = %s WHERE product_id = %s",
                (quantity, live_time, product_id)
            )
            conn.commit()
            
            print(f"🛒 [External POS] Sold {quantity}x {product_id}")
            
            # Wait 2 to 5 seconds before the next "customer" buys something
            time.sleep(random.uniform(2.0, 5.0))

    except KeyboardInterrupt:
        print("\n🛑 Live simulation stopped manually.")
    except Error as e:
        print(f"❌ Database error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("🔌 Connection closed.")

if __name__ == "__main__":
    seed_external_database()