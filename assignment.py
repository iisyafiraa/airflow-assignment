from bs4 import BeautifulSoup
import requests
import pandas as pd
import csv
import json
import os
import sqlalchemy as sa
from airflow.models.param import Param
from airflow.decorators import dag, task, branch_task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import Table, Column, Integer, String, MetaData
from datetime import datetime

# DAG definition
@dag(
    params={
        "url": Param("https://cottonink.co.id/collections/women", description="URL link to extract data"),
        "filename": Param("fashion_collections", description="filename"),
        "file_type": Param("csv", description="File type to extract (csv or json)"),
        "source_type": Param("web", description="Source type (e.g., web, api, etc.)")
    }
)
def assignment_airflow():

    # Define tasks
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    @task()
    def extract_data(**kwargs):
        df = pd.DataFrame()  
        try:
            # Get params from kwargs
            url = kwargs['params']['url']
            filename = kwargs['params']['filename']

            response_fashion = requests.get(url)
            response_fashion.raise_for_status()

            if response_fashion.status_code == 200:
                soup_fashion = BeautifulSoup(response_fashion.content, 'html.parser')
                product_items = soup_fashion.find_all('div', class_='ProductItem__Info ProductItem__Info--center')

                # Save product data to list
                results_fashion = []
                for item in product_items:
                    data = {
                        'product_name': item.find('a').text.strip() if item.find('a') else None,
                        'price': item.find('span').text.strip() if item.find('span') else None,
                        'url': item.find('a')['href'] if item.find('a') else None
                    }
                    results_fashion.append(data)
                df = pd.DataFrame(results_fashion)

                folder_path = '/opt/airflow/data'
                os.makedirs(folder_path, exist_ok=True)
                file_path = os.path.join(folder_path, f"{filename}.csv")
                df.to_csv(file_path, index=False)

                print(f"Data saved to {file_path}")

        except Exception as e:
            print(f"Error occurred while scraping {url}: {e}")

        return df

    # Task to read CSV
    @task(task_id='read_csv_task')
    def read_csv(**kwargs):
        folder_path = '/opt/airflow/data'
        filename = kwargs['params']['filename']
        file_path = os.path.join(folder_path, f"{filename}.csv")
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
        print(f"Data dari CSV: {data}")
        return data

    # Task to read JSON
    @task(task_id='read_json_task')
    def read_json(**kwargs):
        folder_path = '/opt/airflow/data'
        filename = kwargs['params']['filename']
        file_path = os.path.join(folder_path, f"{filename}.json")
        with open(file_path, "r") as f:
            data = json.load(f)
        print(f"Data dari JSON: {data}")
        return data

    # Branching task to decide which file to process
    @branch_task
    def choose_file_type(**kwargs):
        file_type = kwargs['params']['file_type']
        if file_type == 'csv':
            return 'read_csv_task'
        else:
            return 'read_json_task'

    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def load_to_sqlite(**kwargs):
        try:
            # Parameter dari DAG
            folder_path = '/opt/airflow/data'
            filename = kwargs['params']['filename']
            file_path = os.path.join(folder_path, f"{filename}.csv")
            
            # Baca data dari CSV
            df = pd.read_csv(file_path)

            # Muat data ke SQLite
            engine = sa.create_engine(f"sqlite:////opt/airflow/data/{filename}.sqlite")
            with engine.begin() as conn:
                df.to_sql(filename, conn, index=False, if_exists="replace")

            print(f"Data berhasil dimuat ke SQLite dengan nama table: {filename}")
        except Exception as e:
            print(f"Terjadi error saat memuat data ke SQLite: {e}")


    # Step 1: Extract data
    extract_data_task = extract_data()

    # Step 2: Read file (based on branching)
    choose_file_type_task = choose_file_type()

    # Step 3: Read CSV or JSON based on file type
    read_csv_task = read_csv(filename="{{ params['filename'] }}")
    read_json_task = read_json(filename="{{ params['filename'] }}")

    # Step 4: Load to SQLite (based on file type)
    # load_sqlite_task = load_to_sqlite(
    #     data="{{ task_instance.xcom_pull(task_ids='read_csv_task') if params['file_type'] == 'csv' else ti.xcom_pull(task_ids='read_json_task') }}", 
    #     table_name="{{ params['filename'] }}"
    # )
    load_sqlite_task = load_to_sqlite()

    # Set up task dependencies
    start_task >> extract_data_task >> choose_file_type_task
    choose_file_type_task >> [read_csv_task, read_json_task]
    [read_csv_task, read_json_task] >> load_sqlite_task >> end_task

assignment_airflow()