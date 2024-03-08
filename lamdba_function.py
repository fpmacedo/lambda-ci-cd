import json
import boto3
import requests
import logging
import time
import csv
import datetime
import re
import unicodedata
from botocore.exceptions import BotoCoreError, ClientError

# Configurando o logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def normalize_text(text):
    """
    Normalize text to NFD form, remove accents, replace spaces with hyphens,
    remove non-alphanumeric characters, and convert to lowercase.
    input: text
    output: clean_text  
    """
    text_without_accents = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode('utf-8')
    text_without_spaces = re.sub(r'\s', '-', text_without_accents)
    clean_text = re.sub(r'[^a-zA-Z0-9-]', '', text_without_spaces)
    return clean_text.lower()

def read_csv_from_s3(s3_client, bucket, key):
    """
    Read CSV file from S3 and return a CSV reader object.
    input: s3_client, bucket, key
    output: csv.reader object
    """
    try:
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        csv_data = csv_obj['Body'].read().decode('utf-8')
        return csv.reader(csv_data.splitlines())
    
    except (BotoCoreError, ClientError) as err:
        logger.error(f'Error reading CSV file from S3: {err}')
        raise

def call_weather_api(lat, lon, start, cnt):
    """
    Call the weather API and return the response data.
    input: lat, lon, start, cnt
    output: response.json()
    """
    api_url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&cnt={cnt}&appid=5645791e289ea974155c5ea490e8db3f'
    for attempt in range(3):
        try:
            response = requests.get(api_url, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as err:
            logger.error(f'Error calling the API: {err}')
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

def save_data_to_s3(s3_client, bucket, key, data):
    """
    Save data to S3.
    input: s3_client, bucket, key, data
    output: None
    """
    try:
        s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as err:
        logger.error(f'Error saving data to S3: {err}')
        raise

def lambda_handler(event, context):
    """
    This is the main handler function for the AWS Lambda service. It is triggered by an event and context.

    Parameters:
    event (dict): The event that triggered this function. It should contain a 'date' key with a timestamp value.
    context (obj): The AWS Lambda context object, contains runtime information.

    The function performs the following steps:
    1. Initializes an S3 client using boto3.
    2. Defines the CSV file and bucket name to be used.
    3. Extracts the date, hour, and minute from the event's timestamp.
    4. Checks if the CSV file name is provided in the event.
    5. Reads the CSV file from S3 and skips the header.
    6. For each row in the CSV file, it normalizes the city name and retrieves the latitude and longitude.
    7. Calls a weather API with the latitude, longitude, and date.
    8. Updates the returned data with the city, latitude, and longitude.
    9. Saves the updated data to S3 in a specific path format.
    10. Logs a message indicating successful data save.

    Returns:
    None
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Define CSV file and bucket name
    csv_file = 'cities.csv'
    bucket_name = 'how-desafio-3'

    # Extract date, hour, and minute from event's timestamp
    date_time = datetime.datetime.fromtimestamp(int(event['date']))
    date, hour, minute = date_time.strftime('%Y-%m-%d %H %M').split()

    # Read CSV file from S3 and skip header
    csv_reader = read_csv_from_s3(s3_client, bucket_name, csv_file)
    next(csv_reader)

    # For each row in CSV file, normalize city name and retrieve latitude and longitude
    for index, row in enumerate(csv_reader):
        city, lat, lon = normalize_text(row[0]), row[1], row[2]

        # Call weather API with latitude, longitude, and date
        data = call_weather_api(lat, lon, event['date'], 1)

        # Update returned data with city, latitude, and longitude
        data.update({'city': city, 'latitude': lat, 'longitude': lon})

        # Save updated data to S3 in specific path format
        save_data_to_s3(s3_client, bucket_name, f'raw/{date}/{hour}/{minute}/{city}.json', data)

    # Log message indicating successful data save
    logger.info('Data successfully saved to S3')