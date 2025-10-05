-- Manually insert some sample sales data for testing
USE bizpulse_db;

INSERT INTO sales_transactions (product_id, quantity, timestamp) VALUES
('PROD_0001', 2, '2023-10-01 10:00:00'),
('PROD_0002', 1, '2023-10-01 11:30:00'),
('PROD_0001', 1, '2023-10-02 14:00:00'),
('PROD_0003', 5, '2023-10-03 09:00:00'),
('PROD_0002', 3, '2023-10-04 16:20:00'),
('PROD_0001', 2, '2023-10-05 10:15:00'),
('PROD_0001', 3, '2023-10-10 12:00:00'),
('PROD_0002', 2, '2023-10-11 13:00:00'),
('PROD_0003', 4, '2023-10-12 18:00:00'),
('PROD_0001', 1, '2023-10-15 11:00:00');