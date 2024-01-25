from datetime import datetime, timezone
from pymongo import MongoClient
import pandas as pd
import yaml, logging
import numpy as np
import plugins.logger_setup

pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns

with open("/opt/airflow/dags/secrets.yaml", "r") as f:
    config = yaml.safe_load(f)

log_file_path = config['log_file_path']
transform_logger = plugins.logger_setup.Logger(name='openweather_etl', log_file=log_file_path).get_logger()

def get_data_from_mongodb(mongo_uri, db_name, collection_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        data = list(collection.find({}))  # Fetch all documents
        client.close()

        # Convert ObjectId to string
        for item in data:
            item['_id'] = str(item['_id'])

        return data
    
    except Exception as e:
        transform_logger.error(f"Error getting data from MongoDB: {e}")
        raise
    
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def transform_main(mongo_uri, db_name, collection_name):
    try:
        # Get data from MongoDB
        data = get_data_from_mongodb(mongo_uri, db_name, collection_name)
    except Exception as e:
        transform_logger.error(f"Error getting data from MongoDB: {e}")
    
    try:
        # Create an empty list to store the flattened data
        flattened_items = []

        # Loop through the data and flatten each item
        for item in data:
            # Flatten the item and add to the new list
            flattened_items.append(flatten_json(item))

        # Create a DataFrame from the flattened list
        df = pd.DataFrame(flattened_items)
        df = df.drop('_id', axis=1)
        df = df.drop('rain_1h', axis=1)
        
        # Convert temperatures from Kelvin to Celsius
        df['main_temp'] = df['main_temp'] - 273.15
        df['main_feels_like'] = df['main_feels_like'] - 273.15
        df['main_temp_min'] = df['main_temp_min'] - 273.15
        df['main_temp_max'] = df['main_temp_max'] - 273.15

        # Convert Unix timestamp to human-readable datetime
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        df['sys_sunrise'] = pd.to_datetime(df['sys_sunrise'], unit='s')
        df['sys_sunset'] = pd.to_datetime(df['sys_sunset'], unit='s')

        # Convert wind speed from m/s to km/h (1 m/s = 3.6 km/h)
        df['wind_speed'] = df['wind_speed'] * 3.6

        replace_with = {'id':'city_id', "name":"city_name"}

        for key, value in replace_with.items():
            df.rename(columns={key:value}, inplace=True)

        return df
    
    except Exception as e:
        transform_logger.error(f"Error transforming data: {e}")