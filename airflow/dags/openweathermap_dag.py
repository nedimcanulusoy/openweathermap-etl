import pandas as pd
from airflow.decorators import dag, task
from openweather_fetch_api import extract_main
from openweather_transform import transform_main
from openweather_load import load_main
import yaml, pendulum, logging
from pymongo import MongoClient
from datetime import timedelta, datetime
import plugins.logger_setup

with open("/opt/airflow/dags/secrets.yaml", "r") as f:
    config = yaml.safe_load(f)

log_file_path = config['log_file_path']
dag_logger = plugins.logger_setup.Logger(name='openweather_etl', log_file=log_file_path).get_logger()


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='openweather_dag',
default_args=default_args, 
schedule_interval='@daily',
#star time 21 jan 2024 06:00 and everyday at 06:00
start_date=pendulum.datetime(2024, 1, 21, 6, 0, 0, tz="Europe/Stockholm"),
catchup=False, tags=['openweather'])

def openweather_api_dag():
    @task()
    def extract():
        try:
            extract_main(config['api_key'], config['latitude'], config['longitude'], config['city'], config['mongo_uri'], config['db_name'], config['collection_name'])
        except Exception as e:
            dag_logger.error(f"Error extracting data from API: {e}")
            raise

    @task()
    def transform():
        try:
            transformed_clean_data = transform_main(config['mongo_uri'], config['db_name'], config['collection_name'])
            return transformed_clean_data

        except Exception as e:
            dag_logger.error(f"Error transforming data: {e}")
            raise

    @task()
    def load(final_data):
        try:
            load_main(config['postgres_uri'], dataframe=final_data)
        except Exception as e:
            dag_logger.error(f"Error loading data into the database: {e}")
            raise

    data = extract()
    transformed_data = transform()
    load(transformed_data)

openweather_api_dag = openweather_api_dag()
