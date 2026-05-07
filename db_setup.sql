-- schema.sql (Combined Initialization Script)
-- Run this script to initialize the complete database schema for BizPulse.

USE bizpulse_db;

-- ============================================================
-- 1. DROP EXISTING STRUCTURES (Reverse Dependency Order)
-- ============================================================
DROP VIEW IF EXISTS inventory;
DROP TABLE IF EXISTS purchase_order_items;
DROP TABLE IF EXISTS purchase_orders;
DROP TABLE IF EXISTS stock_movements;
DROP TABLE IF EXISTS product_settings;
DROP TABLE IF EXISTS product_alerts;
DROP TABLE IF EXISTS association_rules;
DROP TABLE IF EXISTS database_connections;
DROP TABLE IF EXISTS forecast_predictions;
DROP TABLE IF EXISTS sales_transactions;
DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

DROP TABLE IF EXISTS external_sales;
DROP TABLE IF EXISTS external_inventory;
DROP PROCEDURE IF EXISTS generate_sales_data;

-- ============================================================
-- 2. CORE MASTER TABLES
-- ============================================================

-- Central table for user/tenant accounts.
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supplier directory
CREATE TABLE IF NOT EXISTS suppliers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    supplier_name VARCHAR(150) NOT NULL,
    contact_person VARCHAR(100),
    contact_email VARCHAR(150),
    phone VARCHAR(50),
    lead_time_days INT,
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Products master table (The single source of truth for items and stock)
CREATE TABLE IF NOT EXISTS products (
    user_id INT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(80),
    warehouse VARCHAR(80),
    cost_price DECIMAL(10,2) NOT NULL DEFAULT 0,
    sell_price DECIMAL(10,2) NOT NULL DEFAULT 0,
    current_stock INT NOT NULL DEFAULT 0,
    min_stock_level INT NOT NULL DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, sku),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- ============================================================
-- 3. VIEWS
-- ============================================================

-- Legacy Inventory View (Maps Kafka/Forecaster queries to the products table)
CREATE OR REPLACE VIEW inventory AS
SELECT 
    user_id, 
    sku AS product_id, 
    current_stock AS stock_level, 
    updated_at AS last_updated
FROM products;

-- ============================================================
-- 4. TRANSACTION & MOVEMENT TABLES
-- ============================================================

-- Audit log of every stock in/out movement
CREATE TABLE IF NOT EXISTS stock_movements (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(150),
    movement_type VARCHAR(30) NOT NULL,  -- 'in','out','loss','return','audit'
    quantity INT NOT NULL,               -- positive = in, negative = out
    note TEXT,
    reference VARCHAR(100),
    performed_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Log all individual sales transactions
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    timestamp DATETIME NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Purchase order header
CREATE TABLE IF NOT EXISTS purchase_orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    supplier_id INT NOT NULL,
    status ENUM('pending','received','cancelled') DEFAULT 'pending',
    total_amount DECIMAL(12,2) DEFAULT 0,
    expected_delivery DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

-- Purchase order line items
CREATE TABLE IF NOT EXISTS purchase_order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    po_id INT NOT NULL,
    sku VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE
);

-- ============================================================
-- 5. AI, ANALYTICS & SETTINGS TABLES
-- ============================================================

-- Results of demand forecasting model
CREATE TABLE IF NOT EXISTS forecast_predictions (
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_units INT,
    confidence_low FLOAT,
    confidence_high FLOAT,
    PRIMARY KEY (user_id, product_id, forecast_date),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- External database connection details
CREATE TABLE IF NOT EXISTS database_connections (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    db_type VARCHAR(50) NOT NULL,
    db_host VARCHAR(255) NOT NULL,
    db_port INT NOT NULL,
    db_user VARCHAR(100) NOT NULL,
    encrypted_db_password TEXT NOT NULL,
    db_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_tested_at TIMESTAMP NULL,
    is_valid BOOLEAN DEFAULT FALSE,
    UNIQUE(user_id, connection_name),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Predicted stock-outs and other alerts
CREATE TABLE IF NOT EXISTS product_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    alert_message VARCHAR(255) NOT NULL,
    severity VARCHAR(20) DEFAULT 'warning',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Market basket analysis results
CREATE TABLE IF NOT EXISTS association_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    antecedents VARCHAR(255) NOT NULL,
    consequents VARCHAR(255) NOT NULL,
    confidence FLOAT NOT NULL,
    lift FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, antecedents, consequents),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- ============================================================
-- 6. MOCK DATA FOR SYNC WORKER TESTING
-- ============================================================

CREATE TABLE IF NOT EXISTS external_inventory (
    product_id VARCHAR(50) PRIMARY KEY,
    stock_level INT NOT NULL,
    last_updated DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS external_sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    timestamp DATETIME NOT NULL
);

INSERT INTO external_inventory (product_id, stock_level, last_updated) VALUES
('SYNC-PROD-A', 250, NOW()),
('SYNC-PROD-B', 180, NOW()),
('SYNC-PROD-C', 320, NOW());

DELIMITER $$
CREATE PROCEDURE generate_sales_data()
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE days_back INT;
    DECLARE transactions_per_day INT;
    DECLARE j INT;
    DECLARE rand_prod_id VARCHAR(50);
    DECLARE rand_quantity INT;
    DECLARE rand_hour INT;
    DECLARE rand_minute INT;
    DECLARE sale_timestamp DATETIME;

    SET days_back = 30;
    WHILE days_back > 0 DO
        SET transactions_per_day = FLOOR(10 + RAND() * 15);
        SET j = 0;
        WHILE j < transactions_per_day DO
            SET rand_prod_id = ELT(FLOOR(1 + RAND() * 3), 'SYNC-PROD-A', 'SYNC-PROD-B', 'SYNC-PROD-C');
            SET rand_quantity = FLOOR(1 + RAND() * 5);
            SET rand_hour = FLOOR(RAND() * 24);
            SET rand_minute = FLOOR(RAND() * 60);

            SET sale_timestamp = DATE_SUB(NOW(), INTERVAL days_back DAY);
            SET sale_timestamp = DATE_ADD(sale_timestamp, INTERVAL rand_hour HOUR);
            SET sale_timestamp = DATE_ADD(sale_timestamp, INTERVAL rand_minute MINUTE);

            INSERT INTO external_sales (product_id, quantity, timestamp)
            VALUES (rand_prod_id, rand_quantity, sale_timestamp);

            SET j = j + 1;
        END WHILE;
        SET days_back = days_back - 1;
    END WHILE;
END$$
DELIMITER ;

CALL generate_sales_data();