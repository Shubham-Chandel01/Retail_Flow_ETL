from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


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



create_product_dim = MySqlOperator(
    task_id='create_product_dim',
    mysql_conn_id='mysql_conn',
    sql='''CREATE TABLE IF NOT EXISTS product_dimension (
             product_id INT AUTO_INCREMENT PRIMARY KEY,
             stock_code VARCHAR(255),
             description VARCHAR(255)
           );'''
)

create_customer_dim = MySqlOperator(
    task_id='create_customer_dim',
    mysql_conn_id='mysql_conn',
    sql='''CREATE TABLE IF NOT EXISTS customer_dimension (
             customer_id INT AUTO_INCREMENT PRIMARY KEY,
             customer_number INT,
             customer_country VARCHAR(255)
           );'''
)

create_date_dim = MySqlOperator(
    task_id='create_date_dim',
    mysql_conn_id='mysql_conn',
    sql='''CREATE TABLE IF NOT EXISTS date_dimension (
             date_id INT AUTO_INCREMENT PRIMARY KEY,
             invoice_date DATETIME,
             year INT,
             month INT,
             day_of_week INT,
             season VARCHAR(50)
           );'''
)

create_fact_table = MySqlOperator(
    task_id='create_sales_fact',
    mysql_conn_id='mysql_conn',
    sql='''CREATE TABLE IF NOT EXISTS retail_sales_fact (
             fact_id INT AUTO_INCREMENT PRIMARY KEY,
             invoice_no VARCHAR(255),
             product_id INT,
             customer_id INT,
             quantity INT,
             unit_price FLOAT,
             total_spend FLOAT,
             date_id INT,
             FOREIGN KEY (product_id) REFERENCES product(product_id),
             FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
             FOREIGN KEY (date_id) REFERENCES date(date_id)
           );'''
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
