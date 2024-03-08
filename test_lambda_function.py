
import pytest
import os
import boto3
from moto import mock_aws
import lamdba_function
from datetime import datetime, timedelta
import logging

# Configurando o logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def aws(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def test_save_data_to_s3(aws):

    bucket = "how-desafio-3"

    key = "raw/2024-02-01/19/00/belem.json"

    s3 = aws

    s3.create_bucket(Bucket=bucket)

    content = {"city": "sao Paulo", "latitude": "-23.55052", "longitude": "-46.633308"}

    lamdba_function.save_data_to_s3(s3, bucket, key, content)

    json_obj = s3.get_object(Bucket=bucket, Key=key)

    json_data = json_obj['Body'].read().decode('utf-8')

    assert(json_data == '{"city": "sao Paulo", "latitude": "-23.55052", "longitude": "-46.633308"}')



def test_read_csv_from_s3(aws):

    bucket = "how-desafio-3"

    file = "cities.csv"

    s3_res = boto3.resource("s3")

    s3_client = aws

    s3_res.create_bucket(Bucket=bucket)

    object = s3_res.Object(bucket, file)

    content ="belem\nbrasilia\nrio de janeiro\nsao paulo\nsalvador\nfortaleza\nmanaus\ncuritiba\nrecife\nbelo horizonte\n"

    object.put(Body=content)

    csv_reader = lamdba_function.read_csv_from_s3(s3_client, bucket, file)
    rows = list(csv_reader)
    
    assert(len(rows) == 10)

def test_normalize_text():
        
        test_cases = [
            ('Test Text', 'test-text'),
            ('Café Münster', 'cafe-munster'),
            ('123-456', '123-456'),
            ('!@#$%^&*()', '')
        ]
        for input_text, expected_output in test_cases:
            assert(lamdba_function.normalize_text(input_text) == expected_output)

def test_lambda_handler(aws):

    bucket = "how-desafio-3"

    file = "cities.csv"

    s3 = aws

    s3_res = boto3.resource("s3")

    s3.create_bucket(Bucket=bucket)

    s3_res.create_bucket(Bucket=bucket)

    object = s3_res.Object(bucket, file)

    content ="belem;-23.55052;-46.633308\n"

    object.put(Body=content)

    event = {"date": "1706814000"}

    response = lamdba_function.lambda_handler(event, None)

    print(response)