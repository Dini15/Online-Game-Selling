'''
=================================================
Milestone 3

Nama  : Dini Anggriyani
Batch : FTDS-023-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch dan Kibana. Adapun dataset yang dipakai adalah dataset mengenai penjualan game online di berbagai region dari seluruh dunia.
=================================================
'''

#import libraries
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

#defaul parameters
default_args ={
    "owner": "dini",
    "retry": None,
    "start_date":datetime(2024,11,1)
}

#function extract
def extract(**context):
    '''
    Fungsi ini dijalankan untuk mengektraksi data dari postgres. Proses dilakukan dengan mengoneksikan ke database 
    untuk selanjutnya membaca data yang sebelumnya sudah dicopy ke postgres menggunakan syntax SQL.
    '''
    #koneksi ke database
    source_hook = PostgresHook(postgres_conn_id='postgres_airflow1')
    source_conn = source_hook.get_conn()
    
    #baca data di sql
    data_raw = pd.read_sql('SELECT * FROM table_m3', source_conn)
    
    #simpan ke csv
    path='/opt/airflow/dags/P2M3_dini_anggriyani_data_raw.csv'
    data_raw.to_csv(path, index=False)
    
    #export data kotor
    #xcom merupakan fungsi yang digunakan untuk berbagi informasi antar task (path file mentah dikirim ke task berikutnya)
    context['ti'].xcom_push(key='raw_data_path', value=path)

#function cleaning
def transform (**context):
    '''
    Fungsi ini dijalankan untuk melakukan cleaning data terhadap data mentah. Pada dataset ini, cleaning data yang dilakukan meliputi
    pembersihan missing value dan outliers. Handling missing value dilakukan dengan cara di drop dan outliers akan dibiarkan karena jumlah missing value yang tidak 
    lebih dari 2% dan outliers < 5% sehingga pengaruhnya terhadap dataset dianggap tidak terlalu signifikan. Data yang sudah dibersihkan selanjutnya disimpan dengan nama file 
    yang sudah ditentukan. Selanjutnya, data yang sudah bersih disimpan ke dalam file csv
    '''
    #ambil context task instance
    ti = context['ti']
    
    #ambil path data mentah
    data_path = ti.xcom_pull(task_ids='extract_data', key='raw_data_path')
    print(data_path)
    
    #buka sebagai dataframe
    data_raw = pd.read_csv(data_path)
    
    #cleaning
    data_clean = data_raw.dropna()
    data_clean = data_clean.drop_duplicates()
    
    #simpan data clean
    path = '/opt/airflow/dags/P2M3_dini_anggriyani_data_clean.csv'
    data_clean.to_csv(path, index=False)
    
    #export data kotor
    context['ti'].xcom_push(key='clean_data_path', value=path)

#function load
def load (**context):
    '''
    Fungsi ini digunakan untuk membuka koneksi ke elasticsearch. Data yang ada akan diubah ke dalam format json
    agar bisa dibaca oleh system. Setelah diolah, data akan dimasukan ke dalam index Elasticsearch (milestone_3)
    '''
    ti = context['ti']
    
    #ambil path data mentah
    data_path = ti.xcom_pull(task_ids='transform_data', key='clean_data_path')
    print(data_path)
    
    #load jadi dataframe
    data_clean = pd.read_csv(data_path)
    
    #proses load data
    #buat koneksi ke elasticsearch
    es = Elasticsearch('http://elasticsearch:9200')
    if not es.ping():
        print('CONNECTION FAILED')
    
    #load data, per baris
    for i, row in data_clean.iterrows():
        doc=row.to_json()
        res = es.index(index='milestone_3', body=doc)

#detail dag
with DAG(
    'pipeline',
    description = 'Pipeline Milestone 3',
    schedule_interval = '10,20,30 9 * * 6',
    default_args=default_args,
    catchup = False
) as dag:
    
    #task extract
    extract_data=PythonOperator(task_id = 'extract_data',
                                python_callable=extract,
                                provide_context=True)
    #task transform
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform,
        provide_context = True
    )
    
    #task load
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=load,
        provide_context = True
    )
    
    extract_data >> transform_data >> load_data