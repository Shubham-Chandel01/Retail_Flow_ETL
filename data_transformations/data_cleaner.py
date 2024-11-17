from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def convert_invoice_date_to_timestamp(df, date_format="MM/dd/yyyy H:mm"):
    df_cleaned = df.withColumn("InvoiceDate", F.to_timestamp(F.col("InvoiceDate"), date_format))
    return df_cleaned


def filter_outliers(df:DataFrame) -> DataFrame:
    """ removes negative and zeroes from quantity and unit price as they can't be
        negative or zero and also filter out date that is not it correct time-stamp format
     """
    df_cleaned = df.filter((df.Quantity >= 0) & (df.UnitPrice >= 0))
    return df_cleaned


def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["InvoiceNo"])


def handle_missing_values(df: DataFrame) -> DataFrame:
    df_cleaned = df.fillna({"Description": "No description"})
    df_cleaned = df_cleaned.dropna(subset=["CustomerID"])
    return df_cleaned
