from config.config import load_db_config
import logging

# Function to write content in Database
def write_to_mysql(df, table_name):

    try:
        config = load_db_config()
        logging.info("Database config loaded successfully.")

        df.write.format("jdbc") \
            .option("url", config['url']) \
            .option("dbtable", table_name) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .mode("overwrite") \
            .save()
        logging.info(f"Data written successfully to the {table_name} table in MySQL.")

    except Exception as e:
        logging.error(f"An error occurred while writing to MySQL table {table_name}: {e}")
        raise