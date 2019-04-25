import datetime
from datetime import datetime as dt, timedelta
import boto3
import time


def check_dt_hour(key, date_hours_list):
    path = key.split("/")[-6] + "/" + key.split("/")[-5] + "/" + key.split("/")[-4] + "/" + key.split("/")[-3]
    if path in date_hours_list:
        return True
    else:
        return False


def get_date_lists(start_hour, end_hour):
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


def get_list_of_folders(s3_client, bucket, prefix, date_list, date_hours_list):
    list_of_folders = []
    for date in date_list:
        location = prefix + date
        if check_folder_existence(s3_client, bucket, location):
            all_folders = s3_client.list_objects(Bucket=bucket, Prefix=location)['Contents']
            for key in all_folders:
                if key['Key'][-1:] != '/' and check_dt_hour(key['Key'], date_hours_list):
                    files_location = 's3://' + bucket + '/' + '/'.join(key['Key'].split('/')[0:-1])
                    if files_location not in list_of_folders:
                        list_of_folders.append(files_location)
        else:
            continue
    return list_of_folders


def create_file(s3_client, bucket, output_file_location, output_file, files_list):
    f = open(output_file, "w+")
    for item in files_list:
        f.write(str(item) + '\n')
    f.close()
    s3_client.upload_file(output_file, bucket, output_file_location)


if __name__ == "__main__":
    # read input params

    lookback = 2
    bucket = 'analytics-qubole-prod'
    prefix = 'prod-adhoc/warehouse/project_ede_tdr.db/js/'

    #start = sys.argv[4].replace('\r', '')
    bookmark_file_bucket = 'analytics-qubole-prod'
    bookmark_file_location = 'prod-adhoc/warehouse/project_ede_tdr.db/js/'

    # calculate parameters
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    start_hour = dt.strptime('2019-03-25 23:00:00', "%Y-%m-%d %H:%M:%S")
    # create end hour by substracting 2h from the current timestamp(last hourly folder from which we gona read data)
    end_hour = (datetime.datetime.now() - timedelta(hours=lookback)).replace(microsecond=0, second=0, minute=0)
    next_load_start_hour = end_hour + timedelta(hours=1)
    output_file = str(int(time.time())) + "_" + str(end_hour.strftime("%Y_%m_%dT%H_%M_%S")) + ".txt"
    output_file_location = bookmark_file_location + output_file

    date_hours_list, date_list = get_date_lists(start_hour, end_hour)
    locations = get_list_of_folders(s3_client, bucket, prefix, date_list, date_hours_list)
    if not locations:
        print("FILE_NAME=NO_FILES_TO_LOAD")
    else:
        create_file(s3_client, bookmark_file_bucket, output_file_location, output_file, locations)
        print("FILE_NAME=" + output_file)
    print("NEXT_LOAD_START_HOUR=" + str(next_load_start_hour.strftime("%Y-%m-%dT%H:%M:%S")))