import datetime
from datetime import datetime as dt, timedelta
import boto3
import time

s3_client = boto3.client('s3')
lookback = 2
bucket = 'analytics-qubole-prod'
prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/'
paginator = s3_client.get_paginator('list_objects_v2')
files_list = []
date_hours_list = []
date_list = []


def check_dt_hour(key):
    path = key.split("/")[-5] + "/" + key.split("/")[-4] + "/" + key.split("/")[-3] + "/" + key.split("/")[-2]
    if path in date_hours_list:
        return True
    else:
        return False


print(datetime.datetime.now())
start_hour = dt.strptime('2019-03-25 23:00:00', "%Y-%m-%d %H:%M:%S")
# create end hour by substracting 2h from the current timstamp(last hourly folder from which we gona read data)
end_hour = (datetime.datetime.now() - timedelta(hours=lookback)).replace(microsecond=0, second=0, minute=0)
start_date = start_hour.date()
end_date = end_hour.date()
output_file = str(int(time.time())) + "_" + str(end_hour.strftime("%Y_%m_%dT%H_%M_%S")) + ".txt"
output_file_location = prefix + str(int(time.time())) + "_" + str(end_hour.strftime("%Y_%m_%dT%H_%M_%S")) + ".txt"
print("Start: " + str(start_hour) + " End: " + str(end_hour))
print("Start date: " + str(start_date) + " End date: " + str(end_date))
print(output_file)

while start_hour <= end_hour:
    date_hours_list.append(start_hour.strftime('%Y/%m/%d/%H'))
    if start_hour.date().strftime('%Y/%m/%d') not in date_list:
        date_list.append(start_hour.date().strftime('%Y/%m/%d'))
    start_hour += timedelta(hours=1)

for date in date_list:
    location = prefix + date
    result = s3_client.list_objects(Bucket=bucket, Prefix=location)['Contents']
    for key in result:
        if key['Key'][-1:] != '/' and check_dt_hour(key['Key']) == True:
            files_list.append(key['Key'])

# list_to_string = ' '.join(files_list)
# s3_client.put_object(Body=list_to_string, Bucket=bucket, Key=output_file)
f = open(output_file, "w+")
for item in files_list:
    f.write(str(item) + '\n')
f.close()

s3_client.upload_file(output_file, bucket, output_file_location)

print(files_list)


'''
# Ready for AWS:

import datetime
from datetime import datetime as dt, timedelta
import boto3

s3_client = boto3.client('s3')
lookback = 2
bucket = 'analytics-qubole-prod'
prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/'
paginator = s3_client.get_paginator('list_objects_v2')
files_list = []
date_hours_list = []
date_list = []

print(datetime.datetime.now())
start_hour = dt.strptime('2019-03-25 23:00:00', "%Y-%m-%d %H:%M:%S")
#create end hour by substracting 2h from the current timstamp(last hourly folder from which we gona read data)
end_hour = (datetime.datetime.now() - timedelta(hours=lookback)).replace(microsecond=0,second=0,minute=0)
start_date = start_hour.date()
end_date = end_hour.date()
print("Start: " + str(start_hour) + " End: " + str(end_hour))
print("Start date: " + str(start_date) + " End date: " + str(end_date))

while start_hour <= end_hour:
    date_hours_list.append(start_hour.strftime('%Y/%m/%d/%H'))
    if start_hour.date().strftime('%Y/%m/%d') not in date_list:
        date_list.append(start_hour.date().strftime('%Y/%m/%d'))
    start_hour += timedelta(hours=1)

for date in date_list:
    location = prefix+date
    result = s3_client.list_objects(Bucket=bucket, Prefix=location)['Contents']
    for key in result:
        if key['Key'][-1:] != '/':
            files_list.append(key['Key'])

print(files_list)
#print(date_hours_list)
#print(date_list)


'''




'''
def hour_range(start_hour, end_hour):
    while start_hour <= end_hour:
        yield start_hour
        start_hour += timedelta(hours=1)

def date_range(start_date, end_date):
    while start_date <= end_date:
        yield start_date
        start_date += timedelta(days=1)
#date_hours_list = [h.strftime('%Y/%m/%d/%H') for h in hour_range(start_hour, end_hour)]
#date_list = [h.strftime('%Y/%m/%d') for h in date_range(start_date, end_date)]
'''

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