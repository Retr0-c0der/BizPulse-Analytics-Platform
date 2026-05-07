-- ============================================================
--  BizPulse – Migration: New Feature Tables
--  Run once against bizpulse_db
-- ============================================================

USE bizpulse_db;

-- Products master table (richer than inventory – name, price, category etc.)
CREATE TABLE IF NOT EXISTS products (
    user_id         INT NOT NULL,
    sku             VARCHAR(50) NOT NULL,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(80),
    warehouse       VARCHAR(80),
    cost_price      DECIMAL(10,2) NOT NULL DEFAULT 0,
    sell_price      DECIMAL(10,2) NOT NULL DEFAULT 0,
    current_stock   INT NOT NULL DEFAULT 0,
    min_stock_level INT NOT NULL DEFAULT 0,
    description     TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, sku),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Audit log of every stock in/out movement
CREATE TABLE IF NOT EXISTS stock_movements (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id        INT NOT NULL,
    sku            VARCHAR(50) NOT NULL,
    product_name   VARCHAR(150),
    movement_type  VARCHAR(30) NOT NULL,  -- 'in','out','loss','return','audit'
    quantity       INT NOT NULL,           -- positive = in, negative = out
    note           TEXT,
    reference      VARCHAR(100),
    performed_by   VARCHAR(100),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Supplier directory
CREATE TABLE IF NOT EXISTS suppliers (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    user_id        INT NOT NULL,
    supplier_name  VARCHAR(150) NOT NULL,
    contact_person VARCHAR(100),
    contact_email  VARCHAR(150),
    phone          VARCHAR(50),
    lead_time_days INT,
    address        TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Purchase order header
CREATE TABLE IF NOT EXISTS purchase_orders (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    user_id           INT NOT NULL,
    supplier_id       INT NOT NULL,
    status            ENUM('pending','received','cancelled') DEFAULT 'pending',
    total_amount      DECIMAL(12,2) DEFAULT 0,
    expected_delivery DATE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
);

-- Purchase order line items
CREATE TABLE IF NOT EXISTS purchase_order_items (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    po_id      INT NOT NULL,
    sku        VARCHAR(50) NOT NULL,
    quantity   INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE
);
