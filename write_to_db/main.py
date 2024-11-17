from pyspark.sql import SparkSession
from config.config import load_config
from data_loader.db_writer import write_to_mysql
from data_loader.dim_fact_tables import create_product_dimension, create_customer_dimension, create_date_dimension, \
    create_retail_sales_fact
from data.data_loader import  load_parquet_data
import logging


def main():

    try:
        # creating spark session
        spark = SparkSession.builder \
            .appName("load_data") \
            .getOrCreate()

        # load data in df
        config = load_config()
        retail_sales_path = config['retail_sales_transformed_path']
        retail_sales_df = load_parquet_data(spark,retail_sales_path)

        # converting sales data into respective fact and dimension table
        product_dimension = create_product_dimension(retail_sales_df)
        customer_dimension = create_customer_dimension(retail_sales_df)
        date_dimension = create_date_dimension(retail_sales_df)
        retail_sales_fact = create_retail_sales_fact(retail_sales_df)

       # writing tables to db
        write_to_mysql(product_dimension, "product_dimension")
        write_to_mysql(customer_dimension, "customer_dimension")
        write_to_mysql(date_dimension, "date_dimension")
        write_to_mysql(retail_sales_fact, "retail_sales_fact")

    except Exception as e:
        logging.error(f"An error occurred during the execution of the data loading in pipeline: {e}")


if __name__ == "__main__":
    main()