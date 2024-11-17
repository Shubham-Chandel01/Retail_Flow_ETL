from pyspark.sql import SparkSession
from config.config import  load_config
from data.data_loader import get_schema , load_data
from enums.enums import FileFormat
import logging

#stores raw data in parquet format
def main():

    try:
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

        #Write Data in parquet format
        retail_sales_df.write.mode("overwrite").parquet(retail_sales_write_path)

    except Exception as e:
        logging.error(f"An error occurred during the execution of the data ingestion in pipeline: {e}")

if __name__ == "__main__":
    main()