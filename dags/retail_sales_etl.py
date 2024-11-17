from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from config.config import  load_config , load_sql_from_file

default_args = {
    'owner': 'Airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=4)
}

with DAG(
    'retail_sales_etl',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 14),
    catchup=False,
    default_args=default_args
) as dag:
    
    data_ingestion_spark_job = SparkSubmitOperator(
        task_id='data_ingestion_job',
        application='/Users/shubhamchandel/airflow/data_ingestion/main.py',
        conn_id='spark_local',  
        verbose=True,
        env_vars={'PYTHONPATH': '/Users/shubhamchandel/airflow'}
    )

    data_transformation_spark_job = SparkSubmitOperator(
        task_id='data_transformation_job',
        application='/Users/shubhamchandel/airflow/data_transformations/main.py',
        conn_id='spark_local',
        verbose=True,
        env_vars={'PYTHONPATH': '/Users/shubhamchandel/airflow'}
    )

    #load config
    config = load_config()
    product_dim_path = config['sql_files']['create_product_dim']
    customer_dim_path = config['sql_files']['create_customer_dim']
    dim_sql_path = config['sql_files']['create_date_dim']
    sales_fact_sql_path = config['sql_files']['create_sales_fact']

    create_product_dim_sql = load_sql_from_file(product_dim_path)
    create_customer_dim_sql = load_sql_from_file(customer_dim_path)
    create_date_dim_sql = load_sql_from_file(dim_sql_path)
    create_sales_fact_sql = load_sql_from_file(sales_fact_sql_path)

create_product_dim = MySqlOperator(
    task_id='create_product_dim',
    mysql_conn_id='mysql_conn',
    sql=create_product_dim_sql
)

create_customer_dim = MySqlOperator(
    task_id='create_customer_dim',
    mysql_conn_id='mysql_conn',
    sql=create_customer_dim_sql
)

create_date_dim = MySqlOperator(
    task_id='create_date_dim',
    mysql_conn_id='mysql_conn',
    sql=create_date_dim_sql
)

create_fact_table = MySqlOperator(
    task_id='create_sales_fact',
    mysql_conn_id='mysql_conn',
    sql=create_sales_fact_sql
)
load_data_spark_job = SparkSubmitOperator(
    task_id='load_data_job',
    application='/Users/shubhamchandel/airflow/data_loader/main.py',
    conn_id='spark_local',
    verbose=True,
    env_vars={'PYTHONPATH': '/Users/shubhamchandel/airflow'}
)

(data_ingestion_spark_job >> data_transformation_spark_job
 >> [create_product_dim, create_customer_dim, create_date_dim]
 >> create_fact_table >> load_data_spark_job)
