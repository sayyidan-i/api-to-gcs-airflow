import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os
from google.cloud import storage

# Define the DAG
with DAG(
    'etl_csv_files_postgres',
    start_date=datetime(2024, 3, 11),
    schedule= '@monthly'
):
    files = {
        'customer_interactions' : 'https://drive.google.com/file/d/1WG3xelY7LHkBDygWq4qPbC0ZXk1y92hI/view?usp=drive_link',
        'product_details' : 'https://drive.google.com/file/d/1WkgRO3mlmhajPiXFOZtnOnWCXzCr_nKd/view?usp=drive_link',
        'purchase_history' : 'https://drive.google.com/file/d/1QlrrDOGYHNAu-uzDIeWijGyqdJnLFjoV/view?usp=drive_link'
    }
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    def read_csv_from_gdrive(url):
        file_id = url.split('/')[-2]
        url = f'https://drive.google.com/uc?id={file_id}'
        df = pd.read_csv(url)
        print("Sample data from the file: ", df.head())
        return df
    
    @task
    def get_files_customer(**kwargs):
        url = files['customer_interactions']
        df = read_csv_from_gdrive(url)
        return df
    
    @task
    def get_files_product(**kwargs):
        url = files['product_details']
        df = read_csv_from_gdrive(url)
        return df 
    
    @task
    def get_files_purchase(**kwargs):
        url = files['purchase_history']
        df = read_csv_from_gdrive(url)
        return df
    
    @task
    def merge_data(**kwargs):
        task_instance = kwargs['task_instance']
        df_customer = task_instance.xcom_pull('get_files_customer')
        df_product = task_instance.xcom_pull('get_files_product')
        df_purchase = task_instance.xcom_pull('get_files_purchase')
        
        df = df_customer.merge(df_purchase, on='customer_id', how='outer')
        df = df.merge(df_product, on='product_id', how='outer')
        print(df.head())
        
        return df
    

    @task
    def load_to_postgres(**kwargs):
        host = Variable.get('postgres_host')
        port = Variable.get('postgres_port')
        database = Variable.get('postgres_database')
        username = Variable.get('postgres_username')
        password = Variable.get('postgres_password')
        
        conn_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
        conn = create_engine(conn_string)
        print("Connection established")
        
        task_instance = kwargs['task_instance']
        df = task_instance.xcom_pull('merge_data')
        df.to_sql('merged_data', conn, index=False, if_exists='replace')
        print("Data loaded to Postgres")
        
    #start >> get_files_customer() >> get_files_product() >> get_files_purchase() >> merge_data() >> load_to_gcs_task() >> end
    start >> [get_files_customer(), get_files_product(), get_files_purchase()] >> merge_data() >> load_to_postgres() >> end