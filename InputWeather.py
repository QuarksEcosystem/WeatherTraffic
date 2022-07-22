"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with the Amazon Kinesis Firehose API to generate a data stream.
"""

from queue import Empty
import time
import json
import random
import boto3
import pandas as pd

# FIXED arguments
NUMBER_OF_ITERATIONS = 1
NUMBER_OF_MESSAGES_PER_ITERATION = 1
SLEEP_TIME_IN_SECONDS = 60
AWS_ACCESS_KEY_ID = "AKIAYCCU73VYJ7Y3YRVN" 
AWS_SECRET_ACCESS_KEY = "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" 
AWS_REGION = "sa-east-1"
STREAM_NAME = "PUT-S3-m2LCL"
KINESIS_SIZE_LIMIT = 1000000 
import random
import time
import datetime
def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%Y-%m-%d', prop)

def send_chunk(chunk, stream_name, kinesis_client):
    #data = json.dumps(chunk, default=str)
    data_size = len(chunk.encode("utf-8"))

    if data_size > KINESIS_SIZE_LIMIT:
        # If data is too large, we split them in 2 recursively
        new_chunk_size = int(len(chunk) / 2)
        send_chunk(chunk[:new_chunk_size], stream_name, kinesis_client)
        send_chunk(chunk[new_chunk_size:], stream_name, kinesis_client)
    else:
        records = [{"Data": chunk}]
        response = kinesis_client.put_record_batch(DeliveryStreamName=stream_name, Records=records)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print('FAILURE: '+str(response))

#function creates fictional data, as Dict()
def send_data(df, date, stream_name, kinesis_client):
    data ={}
    for ind in df.loc[df['StartTime(UTC)'].str.contains(date)].index:
        print("sending data")
        data = {
            'EventId':str(df['EventId'][ind]),
            'Type':str(df['Type'][ind]), 
            'Severity':str(df['Severity'][ind]), 
            'StartTime(UTC)':str(df['StartTime(UTC)'][ind]), 
            'EndTime(UTC)':str(df['EndTime(UTC)'][ind]), 
            'TimeZone':str(df['TimeZone'][ind]), 
            'LocationLat':str(df['LocationLat'][ind]), 
            'LocationLng':str(df['LocationLng'][ind]), 
            'AirportCode':str(df['AirportCode'][ind]), 
            'City':str(df['City'][ind]), 
            'County':str(df['County'][ind]), 
            'State':str(df['State'][ind]), 
            'ZipCode':str(df['ZipCode'][ind])
         }
        response = kinesis_client.put_record(
            DeliveryStreamName=stream_name,
            Record={
                'Data': json.dumps(data)
            }
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print('FAILURE: '+str(response))
    return


#function puts data on firehose delivery stream
def generate(
            stream_name, kinesis_client
            , iterations, messages_per_iteration, sleep, df
            ):
    randomdate = random_date('2019-01-01', '2019-12-31', random.random())
    send_data(df,randomdate, stream_name, kinesis_client) #generate data
    

if __name__ == '__main__':

    # BOTO3 connect using specified access credentials
    # Swap named credentials to a credentials file.
    firehoseclient = boto3.client(
        'firehose',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    df = pd.read_csv('WeatherEvents.csv')

    #generate data and put messages on firehose
    generate(
        STREAM_NAME, firehoseclient
        , NUMBER_OF_ITERATIONS, NUMBER_OF_MESSAGES_PER_ITERATION
        , SLEEP_TIME_IN_SECONDS
        , df
        )