import yaml
import logging

def load_config():
    try:
        with open("/Users/shubhamchandel/airflow/configs/config.yaml", 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)
        return config

    except Exception as e:
        logging.error(f"Error loading config: {e}")


def load_db_config():
    try:
        with open('/Users/shubhamchandel/airflow/configs/config.yaml', 'r') as file:
            config = yaml.safe_load(file)

        mysql_config = config['mysql']
        return {
            'url': mysql_config['url'],
            'user': mysql_config['user'],
            'password': mysql_config['password']
        }

    except Exception as e:
        logging.error(f"Error loading DB config: {e}")


def load_sql_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return file.read()

    except Exception as e:
        logging.error(f"Error reading SQL file {file_path}: {e}")