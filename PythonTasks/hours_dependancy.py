import datetime
from datetime import datetime as dt, timedelta
import boto3

s3_client = boto3.client('s3')

print(datetime.datetime.now())
start_hour = dt.strptime('2019-03-25 10:00:00', "%Y-%m-%d %H:%M:%S")
#create end hour by substracting 2h from the current timstamp(last hourly folder from which we gona read data)
end_hour = (datetime.datetime.now() - timedelta(hours=2)).replace(microsecond=0,second=0,minute=0)

print("Start: " + str(start_hour) + " End: " + str(end_hour))

def hour_range(start_hour, end_hour):
    while start_hour <= end_hour:
        yield start_hour
        start_hour += timedelta(hours=1)

date_hours_list = [h.strftime('%Y/%m/%d/%H') for h in hour_range(start_hour, end_hour)]
print(date_hours_list)

start_date = start_hour.date()
end_date = end_hour.date()
print("Start date: " + str(start_date) + " End date: " + str(end_date))



'''
import boto3

s3_client = boto3.client('s3')

bucket = 'analytics-qubole-prod'
#Make sure you provide / in the end
prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/20/'  
paginator = s3_client.get_paginator('list_objects_v2')
result = paginator.paginate(Bucket=bucket, Prefix=prefix)['Contents']
for key in result:
    if key['Key'][-1:] != '/':
        files_list = key['Key']
        print(files_list)



import boto3

s3_client = boto3.client('s3')

bucket = 'analytics-qubole-prod'
# Make sure you provide / in the end
prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/20/'
result = s3_client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
for key in result:
    if key['Key'][-1:] != '/':
        print(key['Key'])


def _key_existing_size__list(client, bucket, key):
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            return True
        else:
            return False


key = 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/20/14/PAsss_20190318.tab'
print(_key_existing_size__list(s3_client, bucket, key))
key = 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/20/14/PAsss_20198.tab'
print(_key_existing_size__list(s3_client, bucket, key))


'''