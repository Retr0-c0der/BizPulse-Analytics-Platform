# main.py (TENANT-AWARE)

import os
from datetime import timedelta, datetime
from typing import Annotated, Union, Dict, Any, cast, Sequence, Optional

from cryptography.fernet import Fernet, InvalidToken
from fastapi import Depends, FastAPI, HTTPException, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

import mysql.connector
from mysql.connector import Error, connection
from dotenv import load_dotenv

from passlib.context import CryptContext
from jose import JWTError, jwt
from pydantic import BaseModel 

from data_generator import generate_historical_data_for_user

import io
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import cast, Any

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

# 🐛 FIXED: Swapped `str | None` for `Optional[str]` for Python 3.9 compatibility
class ProductCreate(BaseModel):
    sku: str
    product_name: str
    category: Optional[str] = None
    warehouse: Optional[str] = None
    cost_price: float
    sell_price: float
    current_stock: int = 0
    min_stock_level: int = 0
    description: Optional[str] = None
 
class ProductUpdate(BaseModel):
    product_name: str
    category: Optional[str] = None
    warehouse: Optional[str] = None
    cost_price: float
    sell_price: float
    current_stock: int
    min_stock_level: int
    description: Optional[str] = None
 
class BulkProductImport(BaseModel):
    products: list[ProductCreate]
    skip_duplicates: bool = True
 
class StockAdjustment(BaseModel):
    sku: str
    movement_type: str   # 'in' | 'out' | 'loss' | 'return' | 'audit'
    quantity: int
    note: str
    reference: Optional[str] = None
 
class SupplierCreate(BaseModel):
    supplier_name: str
    contact_person: Optional[str] = None
    contact_email: Optional[str] = None
    phone: Optional[str] = None
    lead_time_days: Optional[int] = None
    address: Optional[str] = None
 
class POItem(BaseModel):
    sku: str
    qty: int
    unit_price: float
 
class PurchaseOrderCreate(BaseModel):
    supplier_id: int
    expected_delivery: Optional[str] = None
    items: list[POItem]

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

def get_user(db: connection.MySQLConnection, username: str) -> Optional[Dict[str, Any]]:
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT id, username, hashed_password, full_name, is_active FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    return cast(Optional[Dict[str, Any]], user)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Security Dependency ---
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], db: connection.MySQLConnection = Depends(get_db)) -> Dict[str, Any]:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: Optional[str] = payload.get("sub")
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

# --- HTML Endpoints ---
@app.get("/")
def read_login_page():
    return FileResponse('frontend/login.html')

@app.get("/register")
def read_register_page():
    return FileResponse('frontend/register.html')

@app.get("/connections")
def read_connections_page():
    return FileResponse('frontend/connections.html')

@app.get("/dashboard")
def read_dashboard_page():
    return FileResponse('frontend/dashboard.html')

# --- API Endpoints ---

@app.post("/api/users/register", response_model=UserOut)
def create_user(user: UserCreate, db: connection.MySQLConnection = Depends(get_db)):
    existing_user = get_user(db, user.username)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    hashed_password = get_password_hash(user.password)
    
    cursor = db.cursor(dictionary=True)
    query = "INSERT INTO users (username, full_name, hashed_password) VALUES (%s, %s, %s)"
    try:
        cursor.execute(query, (user.username, user.full_name, hashed_password))
        db.commit()
        
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

    encrypted_pass = encrypt_password(conn_data.db_password)

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
    user_id = current_user['id']
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
    row1 = cast(Optional[Dict[str, Any]], cursor.fetchone())
    total_products = row1['total_products'] if row1 else 0

    cursor.execute("SELECT SUM(stock_level) as total_units FROM inventory WHERE user_id = %s;", (user_id,))
    row2 = cast(Optional[Dict[str, Any]], cursor.fetchone())
    total_units = row2['total_units'] if row2 and row2['total_units'] else 0

    cursor.execute("""
        SELECT COUNT(*) as sales_last_24h FROM sales_transactions 
        WHERE user_id = %s AND timestamp >= NOW() - INTERVAL 1 DAY;
    """, (user_id,))
    row3 = cast(Optional[Dict[str, Any]], cursor.fetchone())
    sales_last_24h = row3['sales_last_24h'] if row3 else 0

    cursor.execute("SELECT COUNT(*) as low_stock_items FROM inventory WHERE user_id = %s AND stock_level < 50;", (user_id,))
    row4 = cast(Optional[Dict[str, Any]], cursor.fetchone())
    low_stock_items = row4['low_stock_items'] if row4 else 0

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
        SELECT id, product_id, alert_message, severity, created_at 
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

# ── PRODUCT ENDPOINTS ────────────────────────────────────────
 
@app.get("/api/products")
def get_products(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT sku, product_name, category, warehouse,
               cost_price, sell_price, current_stock, min_stock_level, description
        FROM products WHERE user_id = %s ORDER BY sku
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows
 
 
@app.post("/api/products", status_code=201)
def create_product(product: ProductCreate, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    try:
        cursor.execute("""
            INSERT INTO products (user_id, sku, product_name, category, warehouse,
                                  cost_price, sell_price, current_stock, min_stock_level, description)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (user_id, product.sku, product.product_name, product.category, product.warehouse,
              product.cost_price, product.sell_price, product.current_stock, product.min_stock_level, product.description))
 
        cursor.execute("""
            INSERT INTO inventory (user_id, product_id, stock_level, last_updated)
            VALUES (%s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE stock_level = %s, last_updated = NOW()
        """, (user_id, product.sku, product.current_stock, product.current_stock))
 
        db.commit()
    except mysql.connector.IntegrityError:
        raise HTTPException(status_code=409, detail=f"SKU '{product.sku}' already exists.")
    finally:
        cursor.close()
    return {"message": "Product created.", "sku": product.sku}
 
 
@app.put("/api/products/{sku}")
def update_product(sku: str, product: ProductUpdate, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    cursor.execute("""
        UPDATE products SET product_name=%s, category=%s, warehouse=%s,
            cost_price=%s, sell_price=%s, current_stock=%s, min_stock_level=%s, description=%s
        WHERE user_id=%s AND sku=%s
    """, (product.product_name, product.category, product.warehouse,
          product.cost_price, product.sell_price, product.current_stock,
          product.min_stock_level, product.description, user_id, sku))
 
    cursor.execute("""
        UPDATE inventory SET stock_level=%s, last_updated=NOW()
        WHERE user_id=%s AND product_id=%s
    """, (product.current_stock, user_id, sku))
 
    db.commit()
    cursor.close()
    return {"message": "Product updated."}
 
 
@app.delete("/api/products/{sku}")
def delete_product(sku: str, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    cursor.execute("DELETE FROM products WHERE user_id=%s AND sku=%s", (user_id, sku))
    cursor.execute("DELETE FROM inventory WHERE user_id=%s AND product_id=%s", (user_id, sku))
    db.commit()
    cursor.close()
    return {"message": "Product deleted."}
 
 
@app.post("/api/products/bulk", status_code=201)
def bulk_import_products(payload: BulkProductImport, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    imported = 0
    skipped = 0
    for p in payload.products:
        try:
            cursor.execute("""
                INSERT INTO products (user_id, sku, product_name, category, warehouse,
                                      cost_price, sell_price, current_stock, min_stock_level)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (user_id, p.sku, p.product_name, p.category, p.warehouse,
                  p.cost_price, p.sell_price, p.current_stock, p.min_stock_level))
            cursor.execute("""
                INSERT INTO inventory (user_id, product_id, stock_level, last_updated)
                VALUES (%s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE stock_level = %s, last_updated = NOW()
            """, (user_id, p.sku, p.current_stock, p.current_stock))
            imported += 1
        except mysql.connector.IntegrityError:
            if payload.skip_duplicates:
                skipped += 1
            else:
                db.rollback()
                cursor.close()
                raise HTTPException(status_code=409, detail=f"Duplicate SKU: {p.sku}")
    db.commit()
    cursor.close()
    return {"imported": imported, "skipped": skipped}
 
 
# ── STOCK ADJUSTMENTS ────────────────────────────────────────
 
@app.get("/api/stock-movements")
def get_stock_movements(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, sku, product_name, movement_type, quantity, note, reference, performed_by, created_at
        FROM stock_movements WHERE user_id = %s ORDER BY created_at DESC LIMIT 100
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows
 
 
@app.post("/api/stock-adjustments", status_code=201)
def adjust_stock(adj: StockAdjustment, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
 
    cursor.execute("SELECT current_stock, product_name FROM products WHERE user_id=%s AND sku=%s", (user_id, adj.sku))
    product = cast(Optional[Dict[str, Any]], cursor.fetchone())
    if not product:
        cursor.close()
        raise HTTPException(status_code=404, detail="Product not found.")
 
    delta = adj.quantity if adj.movement_type in ('in', 'return') else -adj.quantity
    new_stock = product['current_stock'] + delta
    if new_stock < 0:
        cursor.close()
        raise HTTPException(status_code=400, detail="Adjustment would result in negative stock.")
 
    cursor.execute("UPDATE products SET current_stock=%s WHERE user_id=%s AND sku=%s", (new_stock, user_id, adj.sku))
    cursor.execute("UPDATE inventory SET stock_level=%s, last_updated=NOW() WHERE user_id=%s AND product_id=%s", (new_stock, user_id, adj.sku))
    cursor.execute("""
        INSERT INTO stock_movements (user_id, sku, product_name, movement_type, quantity, note, reference, performed_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (user_id, adj.sku, product['product_name'], adj.movement_type, delta, adj.note, adj.reference, current_user['username']))
 
    db.commit()
    cursor.close()
    return {"message": "Stock adjusted.", "new_stock": new_stock}
 
 
# ── SUPPLIERS ────────────────────────────────────────────────
 
@app.get("/api/suppliers")
def get_suppliers(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM suppliers WHERE user_id=%s ORDER BY supplier_name", (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows
 
 
@app.post("/api/suppliers", status_code=201)
def create_supplier(supplier: SupplierCreate, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO suppliers (user_id, supplier_name, contact_person, contact_email, phone, lead_time_days, address)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (user_id, supplier.supplier_name, supplier.contact_person, supplier.contact_email,
          supplier.phone, supplier.lead_time_days, supplier.address))
    db.commit()
    cursor.close()
    return {"message": "Supplier added."}
 
 
@app.delete("/api/suppliers/{supplier_id}")
def delete_supplier(supplier_id: int, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    cursor.execute("DELETE FROM suppliers WHERE id=%s AND user_id=%s", (supplier_id, user_id))
    db.commit()
    cursor.close()
    return {"message": "Supplier deleted."}
 
 
# ── PURCHASE ORDERS ──────────────────────────────────────────
 
@app.get("/api/purchase-orders")
def get_purchase_orders(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT po.id, s.supplier_name, po.status, po.total_amount, po.expected_delivery, po.created_at,
               COUNT(poi.id) AS item_count
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_items poi ON po.id = poi.po_id
        WHERE po.user_id = %s
        GROUP BY po.id
        ORDER BY po.created_at DESC
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows
 
 
@app.post("/api/purchase-orders", status_code=201)
def create_purchase_order(po: PurchaseOrderCreate, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor()
    total = sum(item.qty * item.unit_price for item in po.items)
 
    cursor.execute("""
        INSERT INTO purchase_orders (user_id, supplier_id, total_amount, expected_delivery)
        VALUES (%s,%s,%s,%s)
    """, (user_id, po.supplier_id, total, po.expected_delivery))
    po_id = cursor.lastrowid
 
    for item in po.items:
        cursor.execute("""
            INSERT INTO purchase_order_items (po_id, sku, quantity, unit_price)
            VALUES (%s,%s,%s,%s)
        """, (po_id, item.sku, item.qty, item.unit_price))
 
    db.commit()
    cursor.close()
    return {"message": "Purchase order created.", "po_id": po_id}
 
 
@app.post("/api/purchase-orders/{po_id}/receive")
def receive_purchase_order(po_id: int, current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
 
    cursor.execute("SELECT status FROM purchase_orders WHERE id=%s AND user_id=%s", (po_id, user_id))
    po = cast(Optional[Dict[str, Any]], cursor.fetchone())
    if not po:
        cursor.close()
        raise HTTPException(status_code=404, detail="Purchase order not found.")
    if po['status'] != 'pending':
        cursor.close()
        raise HTTPException(status_code=400, detail="PO is already received or cancelled.")
 
    cursor.execute("SELECT sku, quantity FROM purchase_order_items WHERE po_id=%s", (po_id,))
    items = cast(Sequence[Dict[str, Any]], cursor.fetchall())
    for item in items:
        cursor.execute("""
            UPDATE products SET current_stock = current_stock + %s WHERE user_id=%s AND sku=%s
        """, (item['quantity'], user_id, item['sku']))
        cursor.execute("""
            UPDATE inventory SET stock_level = stock_level + %s, last_updated=NOW()
            WHERE user_id=%s AND product_id=%s
        """, (item['quantity'], user_id, item['sku']))
        cursor.execute("""
            INSERT INTO stock_movements (user_id, sku, movement_type, quantity, note, performed_by)
            VALUES (%s,%s,'in',%s,%s,%s)
        """, (user_id, item['sku'], item['quantity'], f"PO-{str(po_id).zfill(4)} received", current_user['username']))
 
    cursor.execute("UPDATE purchase_orders SET status='received' WHERE id=%s", (po_id,))
    db.commit()
    cursor.close()
    return {"message": "PO received and stock updated."}
 
 
# ── REPORTS (CSV download) ───────────────────────────────────
import io
import csv
from fastapi.responses import StreamingResponse
 
@app.get("/api/reports/valuation")
def report_valuation(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT sku, product_name, category, current_stock, cost_price,
               ROUND(cost_price * current_stock, 2) AS total_value
        FROM products WHERE user_id=%s ORDER BY total_value DESC
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return _csv_response(rows, 'inventory_valuation')
 
 
@app.get("/api/reports/movements")
def report_movements(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT created_at, sku, product_name, movement_type, quantity, note, reference, performed_by
        FROM stock_movements WHERE user_id=%s ORDER BY created_at DESC LIMIT 1000
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return _csv_response(rows, 'stock_movements')
 
 
@app.get("/api/reports/low-stock")
def report_low_stock(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT sku, product_name, category, warehouse, current_stock, min_stock_level,
               (min_stock_level - current_stock) AS shortage
        FROM products WHERE user_id=%s AND current_stock < min_stock_level ORDER BY shortage DESC
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return _csv_response(rows, 'low_stock_report')
 
 
@app.get("/api/reports/sales-performance")
def report_sales_performance(current_user: Annotated[dict, Depends(get_current_user)], db: connection.MySQLConnection = Depends(get_db)):
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT product_id, SUM(quantity) AS total_units_sold, COUNT(*) AS transaction_count,
               MIN(timestamp) AS first_sale, MAX(timestamp) AS last_sale
        FROM sales_transactions WHERE user_id=%s
        GROUP BY product_id ORDER BY total_units_sold DESC
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return _csv_response(rows, 'sales_performance')
 
 
def _csv_response(rows: Sequence[Any], name: str) -> StreamingResponse:
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type='text/csv',
        headers={'Content-Disposition': f'attachment; filename="{name}.csv"'}
    )

# ── ROUTE 1: Dismiss an alert ────────────────────────────────
@app.post("/api/alerts/{alert_id}/dismiss")
def dismiss_alert(
    alert_id: int,
    current_user: Annotated[dict, Depends(get_current_user)],
    db: connection.MySQLConnection = Depends(get_db)
):
    user_id = current_user['id']
    cursor = db.cursor()
    cursor.execute(
        "UPDATE product_alerts SET is_active = FALSE WHERE id = %s AND user_id = %s",
        (alert_id, user_id)
    )
    db.commit()
    affected = cursor.rowcount
    cursor.close()
    if affected == 0:
        raise HTTPException(status_code=404, detail="Alert not found.")
    return {"message": "Alert dismissed."}
 
 
# ── ROUTE 2: Enriched inventory (adds product_name) ──────────
@app.get("/api/inventory/enriched")
def get_inventory_enriched(
    current_user: Annotated[dict, Depends(get_current_user)],
    db: connection.MySQLConnection = Depends(get_db)
):
    """
    Returns inventory joined with product names from the products table.
    Falls back to product_id as the name if no matching products row exists.
    """
    user_id = current_user['id']
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            i.product_id,
            i.stock_level,
            COALESCE(p.product_name, i.product_id) AS product_name
        FROM inventory i
        LEFT JOIN products p ON i.user_id = p.user_id AND i.product_id = p.sku
        WHERE i.user_id = %s
        ORDER BY i.product_id
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    return rows
 
 
# ── ROUTE 3: PDF Summary Report ──────────────────────────────
@app.get("/api/reports/summary-pdf")
def report_summary_pdf(
    current_user: Annotated[dict, Depends(get_current_user)],
    db: connection.MySQLConnection = Depends(get_db)
):
    user_id = current_user['id']
    cursor: Any = db.cursor(dictionary=True)
 
    # Fetch KPIs
    cursor.execute("SELECT COUNT(DISTINCT product_id) AS total_products FROM inventory WHERE user_id = %s", (user_id,))
    total_products = cursor.fetchone()['total_products']
 
    cursor.execute("SELECT COALESCE(SUM(stock_level), 0) AS total_units FROM inventory WHERE user_id = %s", (user_id,))
    total_units = cursor.fetchone()['total_units']
 
    cursor.execute("""
        SELECT COUNT(*) AS sales_24h FROM sales_transactions
        WHERE user_id = %s AND timestamp >= NOW() - INTERVAL 1 DAY
    """, (user_id,))
    sales_24h = cursor.fetchone()['sales_24h']
 
    cursor.execute("""
        SELECT COUNT(*) AS low_stock FROM inventory
        WHERE user_id = %s AND stock_level < 50
    """, (user_id,))
    low_stock_count = cursor.fetchone()['low_stock']
 
    # Fetch top 10 low-stock items
    cursor.execute("""
        SELECT p.sku, p.product_name, p.current_stock, p.min_stock_level,
               (p.min_stock_level - p.current_stock) AS shortage
        FROM products p
        WHERE p.user_id = %s AND p.current_stock < p.min_stock_level
        ORDER BY shortage DESC
        LIMIT 10
    """, (user_id,))
    low_stock_items = cursor.fetchall()
 
    # Fetch top 5 sales performers
    cursor.execute("""
        SELECT product_id, SUM(quantity) AS total_sold, COUNT(*) AS transactions
        FROM sales_transactions
        WHERE user_id = %s
        GROUP BY product_id
        ORDER BY total_sold DESC
        LIMIT 5
    """, (user_id,))
    top_sellers = cursor.fetchall()
 
    cursor.close()
 
    # ── Build the PDF ────────────────────────────────────────
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                            rightMargin=2*cm, leftMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    elements = []
 
    title_style = ParagraphStyle('Title', parent=styles['Title'],
                                  fontSize=22, spaceAfter=4, textColor=colors.HexColor('#1e3a5f'))
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'],
                                     fontSize=10, textColor=colors.HexColor('#64748b'), spaceAfter=20)
    section_style = ParagraphStyle('Section', parent=styles['Heading2'],
                                    fontSize=13, spaceBefore=16, spaceAfter=8,
                                    textColor=colors.HexColor('#1e3a5f'))
 
    # Header
    elements.append(Paragraph("BizPulse Analytics", title_style))
    elements.append(Paragraph(
        f"Business Summary Report  ·  {current_user['full_name']}  ·  {datetime.now().strftime('%d %B %Y, %H:%M')}",
        subtitle_style
    ))
 
    # KPI table
    elements.append(Paragraph("Key Performance Indicators", section_style))
    kpi_data = [
        ['Metric', 'Value'],
        ['Total Products', str(total_products)],
        ['Total Units in Stock', f"{int(total_units):,}"],
        ['Sales (Last 24 Hours)', str(sales_24h)],
        ['Low Stock Items', str(low_stock_count)],
    ]
    kpi_table = Table(kpi_data, colWidths=[10*cm, 6*cm])
    kpi_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
        ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
        ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0, 0), (-1, 0), 11),
        ('FONTNAME',   (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE',   (0, 1), (-1, -1), 10),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
        ('ALIGN', (1, 0), (1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING',    (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING',   (0, 0), (-1, -1), 10),
    ]))
    elements.append(kpi_table)
    elements.append(Spacer(1, 0.5*cm))
 
    # Low stock table
    elements.append(Paragraph("Low Stock Items (Top 10)", section_style))
    if low_stock_items:
        ls_data = [['SKU', 'Product Name', 'Current Stock', 'Min Stock', 'Shortage']]
        for item in low_stock_items:
            ls_data.append([
                item['sku'],
                item['product_name'],
                str(item['current_stock']),
                str(item['min_stock_level']),
                str(item['shortage']),
            ])
        ls_table = Table(ls_data, colWidths=[3*cm, 6*cm, 3*cm, 3*cm, 2.5*cm])
        ls_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#dc2626')),
            ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME',   (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE',   (0, 0), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff5f5')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
            ('ALIGN', (2, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING',    (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING',   (0, 0), (-1, -1), 8),
        ]))
        elements.append(ls_table)
    else:
        elements.append(Paragraph("No low-stock items. All products above minimum levels.", styles['Normal']))
 
    elements.append(Spacer(1, 0.5*cm))
 
    # Top sellers table
    elements.append(Paragraph("Top 5 Sales Performers", section_style))
    if top_sellers:
        ts_data = [['Product ID', 'Total Units Sold', 'Transactions']]
        for item in top_sellers:
            ts_data.append([item['product_id'], str(item['total_sold']), str(item['transactions'])])
        ts_table = Table(ts_data, colWidths=[6*cm, 5*cm, 5*cm])
        ts_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR',  (0, 0), (-1, 0), colors.white),
            ('FONTNAME',   (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME',   (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE',   (0, 0), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING',    (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING',   (0, 0), (-1, -1), 8),
        ]))
        elements.append(ts_table)
    else:
        elements.append(Paragraph("No sales data available yet.", styles['Normal']))
 
    # Footer note
    elements.append(Spacer(1, 1*cm))
    elements.append(Paragraph(
        f"Generated by BizPulse Analytics Platform  ·  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8,
                        textColor=colors.HexColor('#94a3b8'), alignment=TA_CENTER)
    ))
 
    doc.build(elements)
    buffer.seek(0)
 
    filename = f"bizpulse_summary_{datetime.now().strftime('%Y%m%d')}.pdf"
    return StreamingResponse(
        buffer,
        media_type='application/pdf',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )