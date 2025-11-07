-- db_setup.sql
-- Run this script to initialize the database schema for the BizPulse project.

-- Use the database created in docker-compose
USE bizpulse_db;

-- Table to store current inventory levels for each product
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(50) NOT NULL,
    stock_level INT NOT NULL DEFAULT 0,
    last_updated DATETIME NOT NULL,
    PRIMARY KEY (product_id)
);

-- Table to log all individual sales transactions
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id INT AUTO_INCREMENT,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (transaction_id)
    -- Optional: A foreign key can enforce data integrity
    -- FOREIGN KEY (product_id) REFERENCES inventory(product_id)
);

-- Table to store the results of our demand forecasting model
CREATE TABLE IF NOT EXISTS forecast_predictions (
    product_id VARCHAR(50) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_units INT,
    confidence_low FLOAT,
    confidence_high FLOAT,
    -- A composite primary key ensures one forecast per product per day
    PRIMARY KEY (product_id, forecast_date)
);

CREATE TABLE IF NOT EXISTS users (  
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- Optional: Insert initial inventory records to get started
-- You can run this manually or have your stream processor handle it.
-- INSERT INTO inventory (product_id, stock_level, last_updated) VALUES
-- ('PROD_0001', 100, NOW()),
-- ('PROD_0002', 150, NOW()),
-- ('PROD_0003', 80, NOW());