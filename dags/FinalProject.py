from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract():
    # Asumsi path ke file CSV
    file_path = '/opt/airflow/dags/Movies.csv'
    df = pd.read_csv(file_path)
    return df.to_json()  # Konversi ke JSON untuk passing data antar task

def transform(**kwargs):
    ti = kwargs['ti']
    data_str = ti.xcom_pull(task_ids='extract')
    df = pd.read_json(data_str)
    
    # Logika transformasi
    def classify_year(released_year):
        if 1900 <= released_year <= 1999:
            return 'Film Lama'
        elif 2000 <= released_year <= 2012:
            return 'Film Menengah'
        elif 2013 <= released_year <= 2024:
            return 'Film Baru'
    
    df['bmi_group'] = df['Released_year'].apply(classify_year)
    return df.to_json()

def load(**kwargs):
    ti = kwargs['ti']
    data_str = ti.xcom_pull(task_ids='transform')
    df = pd.read_json(data_str)
    
    hook = PostgresHook(postgres_conn_id='postgres_chinook')
    engine = hook.get_sqlalchemy_engine()
    
    # Ganti 'nama_tabel' dengan nama tabel yang sesuai di database Anda
    table_name = 'movies_transformed'
    
    # Memuat data ke dalam tabel PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

with DAG(
    'movie_etl',
    default_args=default_args,
    description='A simple movie ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 4, 22),
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task