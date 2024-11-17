from pyspark.sql import functions as F


# Function to create Product Dimension Table
def create_product_dimension(df):
    return df.select(
        F.col("StockCode"),
        F.col("Description")
    ).dropDuplicates()


# Function to create Customer Dimension Table
def create_customer_dimension(df):
    return df.select(
        F.col("CustomerID").alias("customer_number"),
        F.col("Country").alias("customer_country")
    ).dropDuplicates()


# Function to create Date Dimension Table
def create_date_dimension(df):
    return df.select(
        F.col("InvoiceDate").alias("invoice_date"),
        F.col("Year"),
        F.col("Month"),
        F.col("DayOfWeek").alias("day_of_week"),
        F.col("Season")
    ).dropDuplicates()


# Function to create the Retail Sales Fact Table
def create_retail_sales_fact(df):
    return df.select(
        F.col("InvoiceNo").alias("invoice_number"),
        F.col("CustomerID").alias("customer_id"),
        F.col("StockCode").alias("product_id"),
        F.col("InvoiceDate").alias("date_key"),
        F.col("Quantity"),
        F.col("UnitPrice").alias("unit_price"),
        F.col("TotalSpend").alias("total_spend")
    )
