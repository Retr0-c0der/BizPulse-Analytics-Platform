# main.py (TENANT-AWARE)

import os
from datetime import timedelta, datetime
from typing import Annotated, Union

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Depends, FastAPI, HTTPException, status, BackgroundTasks # <--- CORRECTED
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

import mysql.connector
from mysql.connector import Error, connection
from dotenv import load_dotenv

from passlib.context import CryptContext
from jose import JWTError, jwt
from pydantic import BaseModel # <--- CORRECTED

from data_generator import generate_historical_data_for_user

load_dotenv()

# --- Security Config ---
SECRET_KEY = os.getenv("SECRET_KEY", "a_very_secret_key_that_should_be_in_your_env_file")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --- Database Config ---
DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key().decode())
cipher_suite = Fernet(ENCRYPTION_KEY.encode())

class ConnectionCreate(BaseModel):
    connection_name: str
    db_type: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

class ConnectionOut(BaseModel):
    id: int
    connection_name: str
    db_type: str
    db_host: str
    db_port: int
    db_user: str
    db_name: str
    is_valid: bool

class UserCreate(BaseModel):
    username: str
    full_name: str
    password: str

class UserOut(BaseModel):
    id: int
    username: str
    full_name: str

def encrypt_password(password: str) -> str:
    return cipher_suite.encrypt(password.encode()).decode()

def decrypt_password(encrypted_password: str) -> str:
    try:
        return cipher_suite.decrypt(encrypted_password.encode()).decode()
    except InvalidToken:
        return "Error: Could not decrypt password."
    
# --- Database Dependency ---
def get_db():
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

# --- Helper Functions ---
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(db: connection.MySQLConnection, username: str):
    cursor = db.cursor(dictionary=True)
    # MODIFIED: Select the user's ID as well
    cursor.execute("SELECT id, username, hashed_password, full_name, is_active FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    return user

def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Security Dependency ---
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
    
    user = get_user(db, username=username)
    
    if user is None:
        raise credentials_exception
    return user

# --- FastAPI App ---
app = FastAPI()
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# --- HTML Endpoints ---
@app.get("/")
def read_login_page():
    return FileResponse('frontend/login.html')

@app.get("/register") # <-- NEW ENDPOINT
def read_register_page():
    return FileResponse('frontend/register.html')

@app.get("/connections")
def read_connections_page():
    return FileResponse('frontend/connections.html')

@app.get("/dashboard")
def read_dashboard_page():
    return FileResponse('frontend/dashboard.html')

# --- API Endpoints (NOW TENANT-AWARE) ---

@app.post("/api/users/register", response_model=UserOut) # <-- NEW ENDPOINT
def create_user(user: UserCreate, db: connection.MySQLConnection = Depends(get_db)):
    # Check if user already exists
    existing_user = get_user(db, user.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    hashed_password = get_password_hash(user.password)
    
    cursor = db.cursor(dictionary=True)
    query = "INSERT INTO users (username, full_name, hashed_password) VALUES (%s, %s, %s)"
    try:
        cursor.execute(query, (user.username, user.full_name, hashed_password))
        db.commit()
        
        # Retrieve the newly created user to return its data
        new_user_id = cursor.lastrowid
        cursor.execute("SELECT id, username, full_name FROM users WHERE id = %s", (new_user_id,))
        created_user = cursor.fetchone()

        return created_user

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail="Database error occurred")
    finally:
        cursor.close()

@app.post("/api/connections")
def add_connection(
    conn_data: ConnectionCreate, 
    current_user: Annotated[dict, Depends(get_current_user)], 
    db: connection.MySQLConnection = Depends(get_db)
):
    user_id = current_user['id']
    is_valid = False
    
    # Step 1: Test the external connection
    try:
        test_conn = mysql.connector.connect(
            host=conn_data.db_host, port=conn_data.db_port,
            user=conn_data.db_user, password=conn_data.db_password,
            database=conn_data.db_name, connection_timeout=5
        )
        if test_conn.is_connected():
            is_valid = True
            test_conn.close()
    except Error as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {e}")

    # Step 2: Encrypt the password
    encrypted_pass = encrypt_password(conn_data.db_password)

    # Step 3: Save to our database
    cursor = db.cursor()
    query = """
    INSERT INTO database_connections 
    (user_id, connection_name, db_type, db_host, db_port, db_user, encrypted_db_password, db_name, is_valid, last_tested_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE 
    db_host=%s, db_port=%s, db_user=%s, encrypted_db_password=%s, db_name=%s, is_valid=%s, last_tested_at=NOW()
    """
    params = (
        user_id, conn_data.connection_name, conn_data.db_type, conn_data.db_host, conn_data.db_port, 
        conn_data.db_user, encrypted_pass, conn_data.db_name, is_valid,
        conn_data.db_host, conn_data.db_port, conn_data.db_user, encrypted_pass, conn_data.db_name, is_valid
    )
    cursor.execute(query, params)
    db.commit()
    cursor.close()
    
    return {"message": "Connection tested successfully and saved!"}

@app.get("/api/connections", response_model=list[ConnectionOut])
def get_connections(
    current_user: Annotated[dict, Depends(get_current_user)], 
    db: connection.MySQLConnection = Depends(get_db)
):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    query = "SELECT id, connection_name, db_type, db_host, db_port, db_user, db_name, is_valid FROM database_connections WHERE user_id = %s"
    cursor.execute(query, (user_id,))
    connections = cursor.fetchall()
    cursor.close()
    return connections

@app.post("/token")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], db: connection.MySQLConnection = Depends(get_db)):
    user = get_user(db, form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/api/users/me/generate-demo-data")
def generate_demo_data(
    current_user: Annotated[dict, Depends(get_current_user)],
    background_tasks: BackgroundTasks
):
    """
    Triggers a background task to generate demo data for the current user.
    """
    user_id = current_user['id']
    
    # Run the time-consuming data generation process in the background
    background_tasks.add_task(generate_historical_data_for_user, user_id)
    
    return {"message": f"Demo data generation started for user {user_id}. The dashboard will update shortly."}

@app.get("/api/users/me")
def read_users_me(current_user: Annotated[dict, Depends(get_current_user)]):
    return {
        "id": current_user["id"],
        "username": current_user["username"],
        "full_name": current_user["full_name"],
        "is_active": current_user["is_active"]
    }

@app.get("/api/summary")
def get_summary(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    
    cursor.execute("SELECT COUNT(DISTINCT product_id) as total_products FROM inventory WHERE user_id = %s;", (user_id,))
    total_products = cursor.fetchone()['total_products']

    cursor.execute("SELECT SUM(stock_level) as total_units FROM inventory WHERE user_id = %s;", (user_id,))
    total_units = cursor.fetchone()['total_units'] or 0

    cursor.execute("""
        SELECT COUNT(*) as sales_last_24h FROM sales_transactions 
        WHERE user_id = %s AND timestamp >= NOW() - INTERVAL 1 DAY;
    """, (user_id,))
    sales_last_24h = cursor.fetchone()['sales_last_24h']

    cursor.execute("SELECT COUNT(*) as low_stock_items FROM inventory WHERE user_id = %s AND stock_level < 50;", (user_id,))
    low_stock_items = cursor.fetchone()['low_stock_items']

    cursor.close()
    
    return {
        "total_products": total_products,
        "total_units_in_stock": total_units,
        "sales_last_24h": sales_last_24h,
        "low_stock_items": low_stock_items,
    }

@app.get("/api/inventory")
def get_inventory_status(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT product_id, stock_level FROM inventory WHERE user_id = %s ORDER BY product_id;", (user_id,))
    inventory = cursor.fetchall()
    cursor.close()
    return inventory

@app.get("/api/alerts")
def get_active_alerts(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    query = """
        SELECT product_id, alert_message, severity, created_at 
        FROM product_alerts 
        WHERE user_id = %s AND is_active = TRUE 
        ORDER BY severity DESC, created_at DESC
    """
    cursor.execute(query, (user_id,))
    alerts = cursor.fetchall()
    cursor.close()
    return alerts

@app.get("/api/forecast/{product_id}")
def get_product_forecast(product_id: str, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    query = """
        SELECT forecast_date, predicted_units FROM forecast_predictions 
        WHERE user_id = %s AND product_id = %s ORDER BY forecast_date;
    """
    cursor.execute(query, (user_id, product_id))
    forecast = cursor.fetchall()
    cursor.close()
    return forecast

@app.get("/api/insights/frequently-bought-together")
def get_association_rules(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    query = """
        SELECT antecedents, consequents, confidence 
        FROM association_rules 
        WHERE user_id = %s 
        ORDER BY confidence DESC, lift DESC LIMIT 5
    """
    cursor.execute(query, (user_id,))
    rules = cursor.fetchall()
    cursor.close()
    return rules