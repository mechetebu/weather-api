import requests
import json
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
import logger
import csv
import io

# Load environmental variables
load_dotenv()

# Setup logger for the entire ingestion process
logging = logger.setup_logging(log_to_file=True)

# define constants
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = "Tampa"
zip_code = 33647
country_code = "USA"
ZIP_CODE_CURRENT_API = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=imperial"
bucket_name = "1daatabucket"
s3 = boto3.client("s3")
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
schema = "Raw_api_data"
key = f"{schema}/{CITY}_Weather_{timestamp}.json"


def get_current_weather(URI: str) -> json:
    r = requests.get(URI)
    if r.status_code != 200:
        return logging.exception(f"Code failed due to API status: {r.status_code}. Please retry")
    else:
        data = r.json()
        logging.info(data)
        return data


def load_json_to_s3(data: json, key, bucket=bucket_name) -> None:
    try:
        logging.info(f'Current date_time: {datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')
        # filename = f'Raw_api_data/{CITY}_Weather_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json'
        s3.put_object(Body=json.dumps(data), Bucket=bucket, Key=key)
    except Exception:
        logging.exception(f"Failed to upload: {key}", exc_info=True)
    finally:
        logging.info(f"The file: {key} was uploaded at {datetime.now()}.")


def ingest(get_current_weather, load_json_to_s3, *args, **kwargs):
    """
    Wrapper function for functions involving ingestion.
    """
    get_current_weather_kwargs = {"URI": kwargs.pop("URI")}
    load_json_to_s3_kwargs = {"key": kwargs.pop("key")}
    data = get_current_weather(*args, **get_current_weather_kwargs)
    load_json_to_s3(data, **load_json_to_s3_kwargs)


def flatten_raw_api_data(data: dict) -> json:
    record = {}
    for key, value in data.items():
        if isinstance(value, dict):
            record.update(value)
        elif isinstance(value, list):

            record.update(value[0])
        else:
            record[key] = value
    return json.dumps(record, indent=2)


def convert_json_to_csv(data):
    data_dict = json.loads(data)
    logging.info(f"Verifying data datatype {type(data)} is dictionary")
    columns = data_dict.keys()
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=columns)
    writer.writeheader()
    writer.writerow(data_dict)
    csv_buffer.seek(0)
    return io.BytesIO(csv_buffer.getvalue().encode("utf-8"))


# Processing step
def retrieve_json_from_S3(key, bucket_name) -> dict:
    response = s3.get_object(Bucket=bucket_name, Key=key)
    json_content = response["Body"].read().decode("utf-8")
    data = json.loads(json_content)
    return data


def process(retrieve_json_from_S3, flatten_raw_api_data, **kwargs) -> json:
    """
    Wrapper function for functions involving process
    Returns json to be used for storage
    """
    retrieve_json_from_S3_kwargs = {
        "key": kwargs.pop("key"),
        "bucket_name": kwargs.pop("bucket_name"),
    }
    data = retrieve_json_from_S3(**retrieve_json_from_S3_kwargs)

    return flatten_raw_api_data(data)


def upload_buffer_to_s3(buffer, bucket_name, object_key):
    """
    Uploads a file-like buffer object to S3.
    """
    s3_client = boto3.client("s3")
    buffer.seek(0)  # Rewind buffer to the beginning
    s3_client.upload_fileobj(buffer, bucket_name, object_key)
    logging.info(f"Successfully uploaded to s3://{bucket_name}/{object_key}")


if __name__ == "__main__":
    ingest(get_current_weather, load_json_to_s3, URI=ZIP_CODE_CURRENT_API, key=key)
    processed_data = process(retrieve_json_from_S3, flatten_raw_api_data, key=key, bucket_name=bucket_name)
    logging.info(f"The processed data: {processed_data}")
    logging.info(f"The file: {key} has been processed successfully.")
    tabular_data = convert_json_to_csv(processed_data)
    schema = "Tabularized_data"
    load_key = f"{schema}/{CITY}_Weather_{timestamp}.csv"
    upload_buffer_to_s3(tabular_data, bucket_name, load_key)
    # load_json_to_s3(tabular_data.getvalue(), load_key)
    logging.info(f"The file: {load_key} has been loaded successfully.")


"""
TODO
add pytest
Write the load data function and save it as a csv for ingestion.
stretch - load to a postgres db (This api doesn't yield enough data for this to be a good idea, maybe another project.)
Improve logging and add docstrings and other stretch goals
"""
