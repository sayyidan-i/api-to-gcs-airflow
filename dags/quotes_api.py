import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id = 'quotes_api',
    schedule='@monthly',
    start_date=datetime(2024, 3, 11)
)

def get_quotes(**kwargs):
    url = 'https://type.fit/api/quotes'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("Success got :", data)
    else:
        data = []
        print("Failed fetch:", response.text)
    return data

def return_quotes(**kwargs):
    import random
    task_instance = kwargs['task_instance']
    quotes = task_instance.xcom_pull('get_quotes_task')
    rand_number = random.randrange(0, len(quotes) -1)
    print(f"YOUR DAILY QUOTES = {quotes[rand_number]['text']} from {quotes[rand_number]['author']}")
    
start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

get_quotes_task = PythonOperator(
    dag=dag,
    task_id='get_quotes_task',
    python_callable=get_quotes
)

return_quotes_task = PythonOperator(
    dag=dag,
    task_id='return_quotes_task',
    python_callable=return_quotes
)

start >> get_quotes_task >> return_quotes_task >> end