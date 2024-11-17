import yaml

def load_config():
    with open("/Users/shubhamchandel/airflow/config/config.yaml", 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


def load_db_config(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        config = yaml.safe_load(file)

    mysql_config = config['mysql']
    return {
        'url': mysql_config['url'],
        'user': mysql_config['user'],
        'password': mysql_config['password']
    }
