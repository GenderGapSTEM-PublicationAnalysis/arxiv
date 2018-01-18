import json
import time
from datetime import date, timedelta
from urllib.parse import urlencode

import boto3
from dateutil import parser

from arxiv_xml import ArxivXML
from config import BASE_URL, AWS_S3_BUCKET

DELAY = 11
KEY_LAST_BATCH_DATE = 'last_batch_date.txt'
LOCAL_BUFFER_DIR = "/tmp/"
FILE_LAST_BATCH_DATE = LOCAL_BUFFER_DIR + KEY_LAST_BATCH_DATE
YESTERDAY = date.today() - timedelta(days=1)

BUCKET = boto3.resource('s3').Bucket(AWS_S3_BUCKET)


def build_url(batch_date, resumption_token=None):
    query_string_params = {
        'verb': 'ListRecords',
        'from': batch_date,
        'until': batch_date,
        'metadataPrefix': 'arXiv'
    }

    if resumption_token is None:
        return BASE_URL + '?' + urlencode(query_string_params)
    else:
        return BASE_URL + '?verb=ListRecords&resumptionToken=' + resumption_token


def dump_json_to_s3(data, file_name):
    print(f'Storing file {file_name} to bucket {AWS_S3_BUCKET}')
    BUCKET.put_object(Key=file_name, Body=json.dumps(data))


def fetch_data(batch_date, resumption_token=None):
    """Creates an ArxivXML object containing at most 1000 metadata records.
    Processes the retrieved records and dumps the object attributes to files in S3 whose names contain the batch date.
    Returns resumption token from the XML which indicates whether the data retrieved was complete."""
    arxiv_xml = ArxivXML()
    url = build_url(batch_date, resumption_token=resumption_token)
    print(url)
    arxiv_xml.process_xml(url)
    suffix = batch_suffix(resumption_token)
    if len(arxiv_xml.metadata) > 0:
        dump_json_to_s3(arxiv_xml.metadata, f'metadata/{batch_date}_{suffix}.json')
    if len(arxiv_xml.missing_metadata) > 0:
        dump_json_to_s3(arxiv_xml.missing_metadata, f'missing_metadata/{batch_date}_{suffix}.json')
    return arxiv_xml.resumption_token


def fetch_batch_for_date(batch_date):
    token = fetch_data(batch_date)
    while token is not None:
        time.sleep(DELAY)
        token = fetch_data(batch_date, resumption_token=token)
    store_batch_date(batch_date)


def batch_suffix(resumption_token):
    if resumption_token is None:
        return '0'
    else:
        return resumption_token.rpartition('|')[2]


def calc_batch_date():
    BUCKET.download_file(KEY_LAST_BATCH_DATE, FILE_LAST_BATCH_DATE)
    with open(FILE_LAST_BATCH_DATE, 'r') as fp:
        date_string = fp.readline().strip()
    last_batch_date = parser.parse(date_string).date()
    if last_batch_date < YESTERDAY:
        current_batch_date = (last_batch_date + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        current_batch_date = None
    print(f'Last batch date was {last_batch_date}, current batch date is {current_batch_date}.')
    return current_batch_date


def store_batch_date(batch_date):
    boto3.resource('s3').Object(AWS_S3_BUCKET, KEY_LAST_BATCH_DATE).put(Body=str.encode(batch_date))


def my_handler(event, context):
    """lambda handler syntax; requires event and context variables"""
    # fetches the batch for the day after the last run and stops afterwards
    batch_date = calc_batch_date()
    if batch_date is not None:
        fetch_batch_for_date(batch_date)


if __name__ == '__main__':
    f"""Fetches metadata updates from Arxiv on a daily basis. Each batch contains the changes from one day.
       A batch can consist of multiple files, containing max 1000 records each.
       Which day had been fetched last is stored in the file {KEY_LAST_BATCH_DATE} in the AWS bucket {AWS_S3_BUCKET}.
       To fetch all data, store a date like '1900-01-01' in this file. 
    """
    next_batch_date = calc_batch_date()
    while next_batch_date is not None:
        fetch_batch_for_date(next_batch_date)
        next_batch_date = calc_batch_date()
