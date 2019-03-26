from __future__ import print_function
import boto3
import logging
import urllib

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def handler(event, context):
    event_data = event["Records"][0]
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    print("source_bucket name is: ", source_bucket)
    source_key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    print("source_key is: ", source_key)
    source_file_name = source_key.split("/")[-1]
    print("source_file_name is: " + source_file_name)

    event_dtm = event_data["eventTime"]
    print("lambda triggered for source_key: ", source_key, "at time: ", event_dtm)

    #date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    event_date_array = event_dtm.split("T")[0].split("-")
    event_hour = event_dtm.split("T")[1].split(":")[0]
    event_year = event_date_array[0]
    event_month = event_date_array[1]
    event_day = event_date_array[2]
    print(
        "eventDTTM: " + event_dtm + " eventYear: " + event_year + " eventMonth: " + event_month + " eventDay: " + event_day +  " eventHour: " + event_hour)

    # copy this file to the DPP_SPEND_RAW folder.
    target_bucket = "big-data-analytics-test"
    dest_key = "LZ/DPP_SPEND_RAW/" + event_year + "/" + event_month + "/" + event_day + "/" + event_hour + "/" + source_file_name
    print("Target location: " + target_bucket + "/" + dest_key)
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    s3.copy_object(Bucket=target_bucket, Key=dest_key, CopySource=copy_source)
    print("file copied: "+source_file_name)

    return {'message': source_key}

'''
def check(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True


if check(s3, target_bucket, dest_key) == True:
    print("File already exists")
else:
    s3.copy_object(Bucket=target_bucket, Key=dest_key, CopySource=copy_source)
    print("file copied: "+source_file_name)
'''