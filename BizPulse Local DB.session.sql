-- Manually insert some sample sales data for testing
USE bizpulse_db;

INSERT INTO users (username, hashed_password, full_name, is_active)
VALUES ('testuser', '$2b$12$EtBOMqvMmyNNxk/hlNxG0e5bDwWmCeF36j/ScJBUa5xCaxJzDbXKq', 'Test User', TRUE);