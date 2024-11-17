from config.config import load_db_config


def write_to_mysql(df, table_name,config_file_path='/Users/shubhamchandel/airflow/config/config.yaml'):

    config = load_db_config(config_file_path)

    df.write.format("jdbc") \
        .option("url", config['url']) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .mode("overwrite") \
        .save()