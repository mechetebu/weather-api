import requests
import json
import sys
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
import logger

#Load environmental variables 
load_dotenv()

#Setup logger for the entire ingestion process
logging = logger.setup_logging(log_to_file=True)

#define constants
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = 'Tampa'
zip_code = 33647
country_code = 'USA'
ZIP_CODE_CURRENT_API = f'https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=imperial'
bucket_name = '1daatabucket'
s3 = boto3.client('s3')


def get_current_weather(URI: str) -> json:
    r = requests.get(URI)
    if  r.status_code != 200:
        return logging.exception(f"Code failed due to API status: {r.status_code}. Please retry")
    else: 
        data = r.json()
        logging.info(data)
        return data
def load_json_to_s3(data: json, key, bucket = bucket_name) -> None:
        try:
            logging.info(f'Current date_time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
            #filename = f'Raw_api_data/{CITY}_Weather_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json'
            s3.put_object(
            Body = json.dumps(data),
            Bucket = bucket,
            Key = key
            )
        except Exception as e :
            logging.exception(f"Failed to upload: {key}", exc_info = True)
        finally:
            logging.info(f'The file: {key} was uploaded at {datetime.now()}.')
def ingest(get_current_weather, load_json_to_s3, *args, **kwargs):
    get_current_weather_kwargs = {'URI': kwargs.pop("URI")}
    load_json_to_s3_kwargs = {'key': kwargs.pop('key')}
    data = get_current_weather(*args, **get_current_weather_kwargs)
    load_json_to_s3(data, **load_json_to_s3_kwargs)
    
def flatten_raw_api_data(data: dict)-> json:
        record = {}
        for key, value in data.items():
            if isinstance(value, dict):
                record.update(value)
            elif isinstance(value, list):
                #print(f'This is the value {value[0]}')
                record.update(value[0])
            else:
                record[key] = value
        return record
if __name__ == "__main__":
    ingest(get_current_weather, load_json_to_s3, URI = ZIP_CODE_CURRENT_API, key = f'Raw_api_data/{CITY}_Weather_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json')
        
        

#Processing step
def retrieve_json_from_S3(key, bucket_name)-> dict:
    response = s3.get_object(Bucket=bucket_name, Key= key)
    json_content = (response['Body'].read().decode('utf-8'))
    data  = json.loads(json_content)
    return data

'''
TODO
set all of this up in a git branch
add pytest
create functions of the current import package
update requirements file for docker
Look into black / flake8
'''