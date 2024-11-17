-- create_date_dim.sql
CREATE TABLE IF NOT EXISTS date_dimension (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice_date DATETIME,
    year INT,
    month INT,
    day_of_week INT,
    season VARCHAR(50)
);
