-- create_product_dim.sql
CREATE TABLE IF NOT EXISTS product_dimension (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(255),
    description VARCHAR(255)
);





