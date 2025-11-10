-- db_setup.sql (Multi-Tenant Version)
-- Run this script to initialize the database schema for the BizPulse SaaS platform.

USE bizpulse_db;

-- Drop existing tables to start fresh (important for redevelopment)
DROP TABLE IF EXISTS forecast_predictions;
DROP TABLE IF EXISTS sales_transactions;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS users;

-- Table to store user/tenant accounts. This is our central table.
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store current inventory levels for each product, PER USER.
CREATE TABLE IF NOT EXISTS inventory (
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    stock_level INT NOT NULL DEFAULT 0,
    last_updated DATETIME NOT NULL,
    PRIMARY KEY (user_id, product_id), -- Composite primary key
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table to log all individual sales transactions, PER USER.
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    timestamp DATETIME NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table to store the results of our demand forecasting model, PER USER.
CREATE TABLE IF NOT EXISTS forecast_predictions (
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_units INT,
    confidence_low FLOAT,
    confidence_high FLOAT,
    PRIMARY KEY (user_id, product_id, forecast_date), -- Composite primary key
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table to securely store external database connection details for each user.
CREATE TABLE IF NOT EXISTS database_connections (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    connection_name VARCHAR(100) NOT NULL,
    db_type VARCHAR(50) NOT NULL, -- e.g., 'MySQL', 'PostgreSQL'
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

-- ===================================================================================
--  TABLES FOR TESTING THE DATA SYNC WORKER
--  These simulate a user's external database with rich historical data.
-- ===================================================================================
DROP TABLE IF EXISTS external_sales;
DROP TABLE IF EXISTS external_inventory;

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

-- Insert starting inventory for the synced products
INSERT INTO external_inventory (product_id, stock_level, last_updated) VALUES
('SYNC-PROD-A', 250, NOW()),
('SYNC-PROD-B', 180, NOW()),
('SYNC-PROD-C', 320, NOW());

-- Stored Procedure to generate realistic historical sales data
DROP PROCEDURE IF EXISTS generate_sales_data;

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

    -- Loop for 30 days of history
    SET days_back = 30;
    WHILE days_back > 0 DO
        -- Generate a random number of sales for that day
        SET transactions_per_day = FLOOR(10 + RAND() * 15);
        SET j = 0;
        WHILE j < transactions_per_day DO
            -- Pick a random product
            SET rand_prod_id = ELT(FLOOR(1 + RAND() * 3), 'SYNC-PROD-A', 'SYNC-PROD-B', 'SYNC-PROD-C');
            -- Random quantity
            SET rand_quantity = FLOOR(1 + RAND() * 5);
            -- Random time
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

-- Execute the procedure to populate the table
CALL generate_sales_data();

-- ===================================================================================

-- ===================================================================================
--  TABLES FOR NEW AI & ANALYTICS FEATURES
-- ===================================================================================

-- Table to store product-specific settings needed for intelligent recommendations.
CREATE TABLE IF NOT EXISTS product_settings (
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    lead_time_days INT DEFAULT 7,          -- Time for new stock to arrive.
    safety_stock_days INT DEFAULT 14,    -- Buffer stock desired.
    PRIMARY KEY (user_id, product_id),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table to store alerts, such as predicted stock-outs.
CREATE TABLE IF NOT EXISTS product_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL, -- e.g., 'PREDICTED_STOCK_OUT'
    alert_message VARCHAR(255) NOT NULL,
    severity VARCHAR(20) DEFAULT 'warning', -- 'warning', 'critical'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Table to store market basket analysis results (association rules).
CREATE TABLE IF NOT EXISTS association_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    antecedents VARCHAR(255) NOT NULL, -- The "if" part, e.g., 'PROD_A'
    consequents VARCHAR(255) NOT NULL, -- The "then" part, e.g., 'PROD_B'
    confidence FLOAT NOT NULL,
    lift FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, antecedents, consequents),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
