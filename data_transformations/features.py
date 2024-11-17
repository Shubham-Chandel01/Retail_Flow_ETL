from pyspark.sql import DataFrame
from pyspark.sql.functions import month, dayofweek, year , when
from pyspark.sql import functions as F

def total_spend_per_transaction( df: DataFrame) -> DataFrame:
    df_cleaned = df.withColumn("TotalSpend", F.round(df.Quantity * df.UnitPrice,2))

    return df_cleaned


def add_date_features( df: DataFrame ) -> DataFrame :
    df_cleaned = df.withColumn("Month", month(df["InvoiceDate"]))
    df_cleaned = df_cleaned.withColumn("DayOfWeek", dayofweek(df["InvoiceDate"]))
    df_cleaned = df_cleaned.withColumn("Year", year(df["InvoiceDate"]))

    return df_cleaned

def add_season(df: DataFrame ) -> DataFrame :

    df_cleaned = df.withColumn("Season",
                                   when((df["Month"].isin([12, 1, 2])), "Winter")
                                   .when((df["Month"].isin([3, 4, 5])), "Spring")
                                   .when((df["Month"].isin([6, 7, 8])), "Summer")
                                   .otherwise("Fall"))
    return df_cleaned