import requests
import json
from pymongo import MongoClient
import yaml, logging
import plugins.logger_setup


with open("/opt/airflow/dags/secrets.yaml", "r") as f:
    config = yaml.safe_load(f)

log_file_path = config['log_file_path']
extract_logger = plugins.logger_setup.Logger(name='openweather_etl', log_file=log_file_path).get_logger()


def get_weather_forecast(api_key, latitude, longitude, city):
    
    #if latitude and longitude is not given, use city
    if latitude == None and longitude == None:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    
    else:
        url = f"http://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}"
        

    try:
        response = requests.get(url)
        response.raise_for_status()  # This will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response.json()
    except requests.exceptions.HTTPError as err:
        extract_logger.error(f"An HTTP error occurred: {err}")
        raise SystemExit(err)


def insert_into_mongodb(collection, data):
    try:
        collection.insert_one(data)
    except Exception as e:
        extract_logger.error(f"Error inserting data into MongoDB: {e}")
        raise

def extract_main(api_key, latitude, longitude, city, mongo_uri, db_name, collection_name):
    # Establishing MongoDB connection
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
    except Exception as e:
        extract_logger.error(f"Error connecting to MongoDB: {e}")
        raise

    try:
        forecast_data = get_weather_forecast(api_key, latitude, longitude, city)
        insert_into_mongodb(collection, forecast_data)
    except Exception as e:
        extract_logger.error(f"Error getting weather forecast data: {e}")
        raise