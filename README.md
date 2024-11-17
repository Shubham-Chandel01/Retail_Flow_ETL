# **Retail Sales ETL Pipeline**

## **Overview-**
This project is an ETL pipeline for transforming and loading retail sales data into a MySQL database. The pipeline uses Apache Airflow, PySpark, and other associated tools to clean, transform, and load the data efficiently.

## **Requirements**
- **Python 3.x**
- **Apache Airflow 2.x or higher**
- **PySpark 3.x or higher**
- **Pandas for data profiling**
- **MySQL for data storage**

## **Setup**
- Install Airflow on mac : https://medium.com/accredian/setting-up-apache-airflow-in-macos-57ab9e8edb8a
- Install Airflow on windows : https://vivekjadhavr.medium.com/how-to-easily-install-apache-airflow-on-windows-6f041c9c80d2

## **Running the DAG**
Once everything is set up, you can trigger the DAG via the Airflow UI (http://localhost:8080)

<img width="1427" alt="Screenshot 2024-11-17 at 11 44 42 PM" src="https://github.com/user-attachments/assets/c0eaa5c7-ce69-44a5-812c-9c17e5035116">



## **Configuration**
You can configure file paths, database credentials, and SQL queries by creating the config.yaml file under the configs/ directory.

## **How It Works**
- **Airflow DAG Overview**
The following screenshot shows the pipeline's DAG in Airflow, illustrating the different tasks in the ETL process:
<img width="1070" alt="Screenshot 2024-11-17 at 11 44 01 PM" src="https://github.com/user-attachments/assets/1d37d28e-4e42-4165-8b6a-3db60ea3f087">

- **Data Ingestion**: Reads retail sales data from uploaded files and writes it in Parquet format using PySpark.
- **Data Transformation:** Applies transformations like handling missing values, removing duplicates, adding date features, and creating new columns (total spend, season).
- **Data Profiling:** Profiles data using Pandas Profiling to generate statistics and reports.
  
  <img width="1427" alt="Screenshot 2024-11-17 at 11 45 24 PM" src="https://github.com/user-attachments/assets/eca08744-80f5-4418-8c77-3ef973df5e26">

- **Data Loading:** Loads cleaned data into MySQL, creating and populating dimension (product, customer, date) and fact tables (retail sales).


  
