-- Manually insert some sample sales data for testing
USE bizpulse_db;

UPDATE inventory
SET stock_level = 5, last_updated = NOW()
WHERE product_id = 'PROD_0001';

SELECT * FROM users 