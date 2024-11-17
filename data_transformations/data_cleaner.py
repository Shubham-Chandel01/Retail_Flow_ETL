from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

# Function to convert InvoiceDate to timestamp
def convert_invoice_date_to_timestamp(df, date_format="MM/dd/yyyy H:mm"):
    try:
        df_cleaned = df.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), date_format))
        logging.info("InvoiceDate column successfully converted to timestamp.")
        return df_cleaned

    except Exception as e:
        logging.error(f"Error converting InvoiceDate to timestamp: {e}")
        raise

# Function to remove outliers from Quantity and UnitPrice
def filter_outliers(df: DataFrame) -> DataFrame:
    try:
        df_cleaned = df.filter((df.Quantity >= 0) & (df.UnitPrice >= 0))
        logging.info("Outliers (negative or zero) in Quantity and UnitPrice removed.")
        return df_cleaned

    except Exception as e:
        logging.error(f"Error filtering outliers in Quantity or UnitPrice: {e}")
        raise

# Function to remove duplicates
def remove_duplicates(df: DataFrame) -> DataFrame:
    try:
        df_cleaned = df.dropDuplicates(["InvoiceNo"])
        logging.info("Duplicates removed based on InvoiceNo.")
        return df_cleaned

    except Exception as e:
        logging.error(f"Error removing duplicates: {e}")
        raise

# Function to handle missing values
def handle_missing_values(df: DataFrame) -> DataFrame:
    try:
        df_cleaned = df.fillna({"Description": "No description"}) \
            .dropna(subset=["CustomerID"])
        logging.info("Missing values handled: 'Description' filled and 'CustomerID' dropped if null.")
        return df_cleaned

    except Exception as e:
        logging.error(f"Error handling missing values: {e}")
        raise