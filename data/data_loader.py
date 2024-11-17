from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType
def get_schema():
    return StructType([
        StructField("InvoiceNo", IntegerType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True)
    ])

def load_data(spark, file_path, schema,file_format):
    return spark.read.format(file_format.value).option("header", True).schema(schema).load(file_path)

def load_parquet_data(spark, file_path):
    return spark.read.parquet(file_path)