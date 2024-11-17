from pyspark.sql import SparkSession
from config.config import load_config
from data.data_loader import get_schema , load_data
from data_transformations.data_cleaner import filter_outliers, remove_duplicates, handle_missing_values, \
    convert_invoice_date_to_timestamp
from enums.enums import FileFormat
from data_transformations.features import add_date_features , total_spend_per_transaction , add_season


def main() :

    spark = SparkSession.builder \
        .appName("RetailSalesTransformation") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()



    config = load_config()
    retail_sales_path = config['retail_sales_write_path']
    retail_sales_write_path = config['retail_sales_transformed_path']

    # Load Data
    schema = get_schema()
    file_format = FileFormat.PARQUET
    retail_sales_df = load_data(spark,retail_sales_path,schema,file_format)


    # clean data
    retail_sales_df = convert_invoice_date_to_timestamp(retail_sales_df)
    retail_sales_df = filter_outliers(retail_sales_df)
    retail_sales_df = remove_duplicates(retail_sales_df)
    retail_sales_df = handle_missing_values(retail_sales_df)


    # feature extraction
    retail_sales_df = add_date_features(retail_sales_df)
    retail_sales_df = total_spend_per_transaction(retail_sales_df)
    retail_sales_df = add_season(retail_sales_df)

    # save data

    retail_sales_df.write.mode("overwrite").parquet(retail_sales_write_path)

    retail_sales_df.show()


if __name__ == "__main__":
    main()