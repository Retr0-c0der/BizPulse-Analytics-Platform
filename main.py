# main.py (FINAL CORRECTED VERSION with Dependency Injection)

import os
from datetime import timedelta, datetime
from typing import Annotated, Union

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

import mysql.connector
from mysql.connector import Error, connection
from dotenv import load_dotenv

from passlib.context import CryptContext
from jose import JWTError, jwt

# --- 1. Load Environment Variables ---
load_dotenv()

# --- 2. Security Configuration ---
SECRET_KEY = os.getenv("SECRET_KEY", "a_very_secret_key_that_should_be_in_your_env_file")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --- 3. Database Configuration ---
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --- 4. Database Dependency (THE KEY CHANGE) ---
def get_db():
    """
    This is a dependency function. FastAPI will call this for every request
    that needs a database connection. It ensures the connection is always
    opened and, crucially, always closed after the request is finished.
    """
    db_conn = None
    try:
        db_conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        yield db_conn
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()

# --- 5. Helper Functions (Now they take 'db' as an argument) ---
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_user(db: connection.MySQLConnection, username: str):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    return user

def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    # ... (This function does not change)
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- 6. Security Dependency Function (Updated to use the 'get_db' dependency) ---
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], db: connection.MySQLConnection = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = get_user(db, username=username) # Use the injected db connection
    
    if user is None:
        raise credentials_exception
    return user

# --- 7. FastAPI App Initialization ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# --- 8. API Endpoints (All updated to use the 'get_db' dependency) ---

@app.get("/")
def read_login_page():
    return FileResponse('frontend/login.html')

@app.post("/token")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], db: connection.MySQLConnection = Depends(get_db)):
    user = get_user(db, form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/dashboard")
def read_dashboard_page():
    return FileResponse('frontend/dashboard.html')

@app.get("/api/inventory")
def get_inventory_status(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT product_id, stock_level FROM inventory ORDER BY product_id;")
    inventory = cursor.fetchall()
    cursor.close()
    return inventory

@app.get("/api/forecast/{product_id}")
def get_product_forecast(product_id: str, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    query = "SELECT forecast_date, predicted_units FROM forecast_predictions WHERE product_id = %s ORDER BY forecast_date;"
    cursor.execute(query, (product_id,))
    forecast = cursor.fetchall()
    cursor.close()
    return forecast

# Add this new endpoint to main.py, before the other /api endpoints

@app.get("/api/summary")
def get_summary(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    
    # 1. Total unique products in inventory
    cursor.execute("SELECT COUNT(DISTINCT product_id) as total_products FROM inventory;")
    total_products = cursor.fetchone()['total_products']

    # 2. Total units in stock
    cursor.execute("SELECT SUM(stock_level) as total_units FROM inventory;")
    total_units = cursor.fetchone()['total_units']

    # 3. Sales in the last 24 hours
    cursor.execute("""
        SELECT COUNT(*) as sales_last_24h 
        FROM sales_transactions 
        WHERE timestamp >= NOW() - INTERVAL 1 DAY;
    """)
    sales_last_24h = cursor.fetchone()['sales_last_24h']

    # 4. Find products with low stock (e.g., less than 50 units)
    cursor.execute("SELECT COUNT(*) as low_stock_items FROM inventory WHERE stock_level < 50;")
    low_stock_items = cursor.fetchone()['low_stock_items']

    cursor.close()
    
    return {
        "total_products": total_products,
        "total_units_in_stock": total_units,
        "sales_last_24h": sales_last_24h,
        "low_stock_items": low_stock_items,
    }