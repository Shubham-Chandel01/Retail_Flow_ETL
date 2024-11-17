-- create_sales_fact.sql
CREATE TABLE IF NOT EXISTS retail_sales_fact (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice_no VARCHAR(255),
    product_id INT,
    customer_id INT,
    quantity INT,
    unit_price FLOAT,
    total_spend FLOAT,
    date_id INT,
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES date(date_id)
);