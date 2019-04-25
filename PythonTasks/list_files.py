import datetime
import sys
from datetime import datetime as dt, timedelta
import boto3
import time
import botocore.exceptions


def check_dt_hour(key, date_hours_list):
    '''
    :param key:
    :param date_hours_list:
    :return:
    '''
    path = key.split("/")[-5] + "/" + key.split("/")[-4] + "/" + key.split("/")[-3] + "/" + key.split("/")[-2]
    if path in date_hours_list:
        return True
    else:
        return False


def get_date_lists(start_hour, end_hour):
    '''
    :param start_hour:
    :param end_hour: current hour
    :return:
    '''
    date_hours_list = []
    date_list = []
    while start_hour <= end_hour:
        date_hours_list.append(start_hour.strftime('%Y/%m/%d/%H'))
        if start_hour.date().strftime('%Y/%m/%d/') not in date_list:
            date_list.append(start_hour.date().strftime('%Y/%m/%d/'))
        start_hour += timedelta(hours=1)
    return date_hours_list, date_list


def check_folder_existence(s3_client, bucket, location):
    try:
        s3_client.list_objects(Bucket=bucket, Prefix=location)['Contents']
    except KeyError as e:
        return False
    return True


def get_file_list(s3_client, bucket, prefix, date_list, date_hours_list):
    files_list = []
    paginator = s3_client.get_paginator('list_objects')
    for date in date_list:
        location = prefix + date
        operation_parameters = {'Bucket': bucket,
                                'Prefix': location}
        if check_folder_existence(s3_client, bucket, location) == True:
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                result = page['Contents']
                for key in result:
                    if key['Key'][-1:] != '/' and check_dt_hour(key ['Key'], date_hours_list) == True:
                        files_list.append(key['Key'])
        else:
            continue
    return files_list


def create_file(s3_client, bucket, output_file_location, output_file, files_list):
    f = open(output_file, "w+")
    for item in files_list:
        f.write(str(item) + '\n')
    f.close()
    s3_client.upload_file(output_file, bucket, output_file_location)


if __name__ == "__main__":
    s3_client = boto3.client('s3')
    lookback = 2
    bucket = 'analytics-qubole-prod'
    prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/'

    print(datetime.datetime.now())
    start_hour = dt.strptime('2019-03-25 23:00:00', "%Y-%m-%d %H:%M:%S")
    # create end hour by substracting 2h from the current timstamp(last hourly folder from which we gona read data)
    end_hour = (datetime.datetime.now() - timedelta(hours=lookback)).replace(microsecond=0, second=0, minute=0)
    start_date = start_hour.date()
    end_date = end_hour.date()
    output_file = str(int(time.time())) + "_" + str(end_hour.strftime("%Y_%m_%dT%H_%M_%S")) + ".txt"
    output_file_location = prefix + output_file
    print("Start: " + str(start_hour) + " End: " + str(end_hour))
    print("Start date: " + str(start_date) + " End date: " + str(end_date))
    print("Output file name: " + output_file)

    date_hours_list, date_list = get_date_lists(start_hour, end_hour)
    files_list = get_file_list(s3_client, bucket, prefix, date_list, date_hours_list)
    create_file(s3_client, bucket, output_file_location, output_file, files_list)
    print("List of files: " + str(files_list))
    #sys.exit(output_file)