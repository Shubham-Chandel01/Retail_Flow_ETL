-- create_customer_dim.sql
CREATE TABLE IF NOT EXISTS customer_dimension (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_number INT,
    customer_country VARCHAR(255)
);