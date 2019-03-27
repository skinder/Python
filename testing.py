import datetime
from datetime import datetime as dt, timedelta
import boto3
import time

lookback = 2
files_list = []
date_hours_list = []
date_list = []
result = ['prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/25/22/', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/26/14/', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/25/21/CommissionJunctionTVLY__CA_20190315.tab', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/25/23/1.txt', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/25/23/CommissionJunctionTVLY__CA_20190315.tab', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/26/05/1.txt', 'prod-adhoc/warehouse/project_ede_tdr.db/js/2019/03/26/15/1.txt']

print(datetime.datetime.now())
start_hour = dt.strptime('2019-03-25 23:00:00', "%Y-%m-%d %H:%M:%S")
#create end hour by substracting 2h from the current timstamp(last hourly folder from which we gona read data)
end_hour = (datetime.datetime.now() - timedelta(hours=lookback)).replace(microsecond=0,second=0,minute=0)
start_date = start_hour.date()
end_date = end_hour.date()
output_file = str(int(time.time())) + "_" + str(end_hour.strftime("%Y_%m_%dT%H_%M_%S")) + ".txt"

print("Start: " + str(start_hour) + " End: " + str(end_hour))
print("Start date: " + str(start_date) + " End date: " + str(end_date))
print(output_file)


while start_hour <= end_hour:
    date_hours_list.append(start_hour.strftime('%Y/%m/%d/%H'))
    if start_hour.date().strftime('%Y/%m/%d') not in date_list:
        date_list.append(start_hour.date().strftime('%Y/%m/%d'))
    start_hour += timedelta(hours=1)

def check_dt_hour(key):
    path = key.split("/")[-5] + "/" + key.split("/")[-4] + "/" + key.split("/")[-3] + "/" + key.split("/")[-2]
    if path in date_hours_list:
        return True
    else:
        return False

for key in result:
    if key[-1:] != "/" and check_dt_hour(key):
        files_list.append(key)
str1 = '/n'.join(files_list)
print(files_list)
print(str1)

f = open(output_file,"w+")

for item in files_list:    # iterate over the list items
   f.write(str(item) + '\n') # write to the file
f.close()

#date_hour = path.split("/")[-5] + "/" + path.split("/")[-4] + "/" + path.split("/")[-3] + "/" + path.split("/")[-2]