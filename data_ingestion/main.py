from pyspark.sql import SparkSession
from config.config import  load_config
from data.data_loader import get_schema , load_data
from enums.enums import FileFormat

def main():

    spark = SparkSession.builder \
        .appName("RetailSalesIngestion") \
        .getOrCreate()

    #load config
    config = load_config()
    retail_sales_path = config['retail_data_file_path']
    retail_sales_write_path = config['retail_sales_write_path']

    #Load Data
    schema = get_schema()
    file_format = FileFormat.CSV
    retail_sales_df = load_data(spark,retail_sales_path,schema,file_format)
    retail_sales_df.show()
    retail_sales_df.write.mode("overwrite").parquet(retail_sales_write_path)


if __name__ == "__main__":
    main()