#! /usr/bin/python
# title           :FBInsightsJob.py
# description     :This script would get insights ad level details from Facebook API
# author          :prshah
# date            :20170401
# version         :1.1
# usage           :Called from parent file FBInsights_prep.sh
# notes           :The parameters are retrieved from SQL server with process name : FB_INSIGHTS_PROCESS
# python_version  :2.6.6
# ==============================================================================
import csv
import datetime
import glob
import re
import sys
import time
from PythonTasks.fb_udfs import *
from argparse import ArgumentParser


class FBInsightsJob():

    def __init__(self):
        """
        Init in FBInsightsJob
        """
        # set timezone as UTC
        # os.environ['TZ']='UTC'
        # assign the current date to variable for hdfs date partition
        self.current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.current_date_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.current_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # Initialize directory setups
        self.s3_meta_path = ""
        self.s3_dir_path = ""
        self.s3_partition_dir = ""
        self.out_data_file_name = "insights_data_%s.csv" % self.current_timestamp
        # Initialize general parameters
        self.proc_name = "FB_INSIGHTS_PROCESS"
        self.list_ad_accounts = []
        self.dict_biz_id_ad_accounts = dict()
        self.param_values = dict()
        self.v_start_date = ""
        self.v_end_date = ""
        self.api_version = ""
        self.fb_access_token = ""
        self.d_tracker = dict()
        self.business_id = ""
        self.git_ver = ""
        self.run_id = ""
        self.environment = ""
        self.s_processed = 0
        self.total_files_generated = 0
        self.max_date = datetime.datetime.strptime("2001-01-01", "%Y-%m-%d").date()

    def getFieldList(self):
        return self.param_values.get('FIELD_LIST').strip()

    def getFieldListInsight(self):
        return self.param_values.get('FIELD_LIST_INSIGHT').strip()

    def getNumAdAccts(self):
        return len(self.list_ad_accounts)

    def getFBAccessToken(self):
        return self.param_values.get('FB_ACCESS_TOKEN')

    def getAPIPrefix(self):
        return self.param_values.get('API_URL_PREFIX')

    def getAPIVersion(self):
        return self.param_values.get('API_VERSION')

    def getBusinessIDList(self):
        return self.param_values.get('BUSINESS_ID').strip()

    def getInsightsRestatement(self):
        return self.param_values.get('INSIGHTS_RESTATEMENT')

    def getRunIDAPIURLComponents(self):
        return self.param_values.get('RUN_ID_API_URL_COMPONENTS').strip()

    def getDownloadAcctIDAPIURLComponents(self):
        return self.param_values.get('DWNLD_ACCT_ID_API_URL_COMPONENTS').strip()

    def getRunIDStatusAPIURLComponents(self):
        return self.param_values.get('RUN_ID_STATUS_API_URL_COMPONENTS').strip()

    def getRestatementStartDate(self):
        return self.param_values.get('RESTATEMENT_START_DATE')

    def getRestatementEndDate(self):
        return self.param_values.get('RESTATEMENT_END_DATE')

    def getMaxETLLoadDate(self):
        return self.param_values.get('MAX_ETL_LOAD_DATE')

    def getExceptionInsightsColumnList(self):
        var1 = self.param_values.get('EXCEPTION_INSIGHTS_COLUMN_LIST')
        print "value for EXCEPTION_INSIGHTS_COLUMN_LIST: %s" % var1
        return str(var1)

    def runProcess(self):
        """
        kickstart the run process
        """
        self.list_ad_accounts = []
        field_list = self.getFieldList()
        ad_account_ids = self.dict_biz_id_ad_accounts.get(self.business_id)
        self.list_ad_accounts = ad_account_ids.split(',')
        print self.list_ad_accounts
        print "start_date is : %s" % self.v_start_date
        print "end_date is : %s" % self.v_end_date
        print "***********************************************************"
        print "completed pulling the add_acct_list"
        print "There are [%d] ad accounts for business id [%s]" % (len(self.list_ad_accounts), self.business_id)
        print "***********************************************************"

        v_loop_control = 0
        self.d_tracker = dict()
        for raw_ad_account in self.list_ad_accounts:
            ad_account = raw_ad_account.strip()
            v_loop_control += 1
            print "Ad Acct: ", v_loop_control
            self.d_tracker['ad_account_id_%s' % ad_account] = {}
            self.d_tracker['ad_account_id_%s' % ad_account]['id'] = ad_account
            self.d_tracker['ad_account_id_%s' % ad_account]['retry_count'] = 0
            self.d_tracker['ad_account_id_%s' % ad_account]['first_attempt'] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S")
            report_run_id_json = self.triggerRunIDAPI(ad_account)
            self.d_tracker['ad_account_id_%s' % ad_account]['report_run_id'] = report_run_id_json['report_run_id']
        return True;

    def triggerRunIDAPI(self, ad_account):

        api_prefix = self.getAPIPrefix()
        api_version = self.getAPIVersion()
        field_list = self.getFieldList()
        # get the run_id API url components from Meta
        dict_run_id_api_url_components = eval(self.getRunIDAPIURLComponents())

        # get the URL data template to compile actual url on the fly
        run_id_api_url = "%s/%s/act_%s/insights" % (api_prefix, api_version, ad_account)

        # get the URL API template to compile actual url on the fly
        run_id_api_data = "fields=%s&%s&%s&%s&%s:'%s'%s:'%s'}&access_token=%s" % (
        field_list, dict_run_id_api_url_components['sort'], \
        dict_run_id_api_url_components['level'], \
        dict_run_id_api_url_components['increment'], \
        dict_run_id_api_url_components['time_range_prefix'], \
        self.v_start_date,
        dict_run_id_api_url_components['time_range_suffix'], \
        self.v_end_date,
        self.fb_access_token)

        report_run_id_json = postReqWithJSONResponse(run_id_api_url, run_id_api_data)
        return report_run_id_json

    def checkStatus(self):

        num_ad_accts_to_process = self.getNumAdAccts()
        api_prefix = self.param_values.get('API_URL_PREFIX')
        api_version = self.param_values.get('API_VERSION')
        d_dummy_tracker_temp = dict(self.d_tracker)
        loop_count = 0
        while num_ad_accts_to_process > 0:
            time.sleep(5)
            d_dummy_tracker = dict(d_dummy_tracker_temp)
            d_dummy_tracker_temp = dict()
            loop_count += 1
            acct = 0
            fail_acct = 0
            num_ad_accts_to_process = len(d_dummy_tracker)
            print "length of dummy tracker  %d " % len(d_dummy_tracker)
            print "Attempt %d" % loop_count
            for array_index, v_tracker_content in enumerate(d_dummy_tracker):
                d_loop_tracker = dict(d_dummy_tracker[v_tracker_content])
                v_loop_ad_account_id = d_loop_tracker['id']
                print "account_id -  %s" % v_loop_ad_account_id
                run_id_status_api = "%s/%s/%s?access_token=%s" % (
                api_prefix, api_version, d_loop_tracker['report_run_id'], self.fb_access_token)
                json_data = importJSON(run_id_status_api)
                retry_status_api_count = 0
                if not json_data or 'async_percent_completion' not in json_data:
                    print "Got an exception for this account"
                    if not json_data:
                        print "No JSON retrieved"
                    else:
                        print json_data
                    print "retying..."
                    while ((retry_status_api_count < 5) and (not json_data)):
                        time.sleep(5)
                        retry_status_api_count += 1
                        print "Attempt %d of 5" % retry_status_api_count
                        json_data = importJSON(run_id_status_api)
                        if retry_status_api_count == 5:
                            return False
                d_loop_tracker['run_id_status_json'] = json_data
                if ((d_loop_tracker['run_id_status_json']['async_percent_completion'] == 100) and (
                        d_loop_tracker['run_id_status_json']['async_status'].upper() == 'JOB COMPLETED')):
                    acct += 1
                    print "Status SUCCESSFULL for Ad Acct %d " % acct
                    num_ad_accts_to_process -= 1
                elif ((d_loop_tracker['retry_count'] <= 5) & (
                        d_loop_tracker['run_id_status_json']['async_status'].upper() == 'FAILED')):
                    fail_acct += 1
                    print "Status FAILED for Ad Acct %d " % fail_acct
                    d_loop_tracker['retry_count'] += 1
                    report_run_id_json = self.triggerRunIDAPI(ad_account)
                    d_loop_tracker['report_run_id'] = report_run_id_json['report_run_id']
                    d_dummy_tracker_temp[v_tracker_content] = d_loop_tracker
                elif ((d_loop_tracker['retry_count'] > 5) & (
                        d_loop_tracker['run_id_status_json']['async_status'].upper() == 'FAILED')):
                    print "Maximum re-try reached for ad_account_id : ", v_loop_ad_account_id
                    print "overall job status : ", d_tracker
                    raise IOError
                else:
                    d_dummy_tracker_temp[v_tracker_content] = d_loop_tracker
                self.d_tracker[v_tracker_content] = d_loop_tracker
            if (loop_count == 15):
                return False
            print "***********************************************************"
            print "Number of accounts to process again %d" % num_ad_accts_to_process
            print "Number of accounts successfully processed %d" % acct
            print "***********************************************************"
        for (k, v) in self.d_tracker.items():
            print k + "    " + ":" + "    " + v['run_id_status_json']['async_status']
        return True

    def downloadAdAcctFiles(self):
        """
        """
        api_prefix = self.param_values.get('API_URL_PREFIX')
        api_version = self.param_values.get('API_VERSION')
        print ("Downloading data for each Account id")
        mkdir_cmd = "mkdir -p download_data"
        mkdir_flag = runCmd(mkdir_cmd)
        if not mkdir_flag:
            print ("Command %s Failed" % mkdir_cmd)
            sys.exit(1)
        dict_dwld_acct_id_api_comp = eval(self.getDownloadAcctIDAPIURLComponents())
        field_list = self.getFieldList()
        field_list_insight = self.getFieldListInsight()
        counter = 0
        field_split_list = field_list.split(',')
        field_split_list_insight = field_list_insight.split(',')
        csv_row = ''
        header_row = ''
        print "Field list insight"
        print field_list_insight
        print "Field split list insight"
        print field_split_list_insight
        dict_exception_insights_column_list = eval(self.getExceptionInsightsColumnList())
        print "Exception columns coming from config file"
        print dict_exception_insights_column_list

        for n, field in enumerate(field_split_list_insight):
            # check if field is part of the exception list and frame the rows with defaults values accordingly

            if (field in dict_exception_insights_column_list.keys()):
                temp_column = "row.get('%s',dict_exception_insights_column_list['%s'])" % (field, field)
            else:
                temp_column = "row['%s']" % field

            if n == 0:
                csv_row += temp_column
            if n > 0 and n < len(field_split_list_insight) - 1:
                csv_row += "," + temp_column + ".strip()"
            if n == len(field_split_list_insight) - 1:
                csv_row += "," + temp_column
                break
        print csv_row
        print "----------------"

        for array_index, v_tracker_content in enumerate(self.d_tracker):
            d_loop_tracker = dict(self.d_tracker[v_tracker_content])
            download_acct_id_api = "%s/%s/%s/%s?limit=%s&fields=%s&access_token=%s" % (
            api_prefix, api_version, d_loop_tracker['report_run_id'], \
            dict_dwld_acct_id_api_comp['endpoint'], \
            dict_dwld_acct_id_api_comp['limit'], \
            field_list, self.fb_access_token)
            acct_id = d_loop_tracker['id']
            report_run_id = d_loop_tracker['report_run_id']
            print "report_run_id %s" % report_run_id
            csv_file = "%s/%s_%s" % ("download_data", acct_id, self.out_data_file_name)
            f = csv.writer(open(csv_file, "wb+"))
            # header_row = "%s,%s,%s,%s" % (field_list,"business_id","report_run_id","ETL_load_timestamp")
            # f.writerow(header_row.split(','))
            print "file_name : %s" % csv_file
            dl_link = True
            next_page_count = 0
            while dl_link:
                next_page_count += 1
                print "Link traversed [%d]" % next_page_count
                try:
                    json_data = importJSON(download_acct_id_api)
                    print json_data
                    if 'data' in json_data:
                        for row in json_data["data"]:
                            print "-----"
                            print json_data["data"]
                            print row['date_stop']
                            print self.max_date
                            if self.max_date < datetime.datetime.strptime(row['date_stop'], "%Y-%m-%d").date():
                                self.max_date = datetime.datetime.strptime(row['date_stop'], "%Y-%m-%d").date()
                                print "new max is %s" % self.max_date
                            print ("&&&&&&&&&&")
                            print (csv_row)
                            # print type(csv_row)
                            print row
                            print (dict_exception_insights_column_list['total_action_value'])
                            print("$$$$$$$$$$$$$$")
                            print (eval(csv_row))
                            eval_csv_row = str(eval(csv_row)[0:(len(eval(csv_row))) - 1])
                            print "*******"
                            print eval_csv_row
                            if ('relevance_score' in row):
                                if ('score' in row['relevance_score']):
                                    score = row.get('relevance_score').get('score')
                                else:
                                    score = dict_exception_insights_column_list['relevance_score']['score']
                                    print "score assigned in else clase", score
                            else:
                                score = dict_exception_insights_column_list['relevance_score']['score']
                                print "score assigned in else clase", score
                            print eval_csv_row
                            eval_csv_row = re.sub("[u()']", '', eval_csv_row)
                            eval_csv_row += ",%s,%s,%s" % (score, self.business_id, report_run_id)
                            eval_csv_row += ",%s" % self.current_date_time
                            # eval_csv_row_trimmed = ",".join(eval_csv_row.split(','))
                            eval_csv_row_trimmed = eval_csv_row.replace(" ", "")
                            # print eval_csv_row_trimmed
                            f.writerow(eval_csv_row_trimmed.split(','))
                except KeyError, e:
                    print 'KeyError - reason "%s"' % str(e)
                    return False
                except ValueError, e:
                    print 'ValueError - reason "%s"' % str(e)
                    return False
                if 'paging' in json_data:
                    print "inside paging"
                    if 'next' in json_data['paging']:
                        print "next exists"
                        dl_link = True
                        download_acct_id_api = json_data['paging']['next']
                    else:
                        dl_link = False

            counter += 1
            self.total_files_generated += 1
            print "Done export_report for acct [%d]" % counter
        return True

    def copyFilesToS3(self):
        """
        This function moves data files to S3
        """
        self.s3_partition_dir = "%sdata/load_date=%s/" % (self.s3_dir_path, self.current_date)
        s3_copy_cmd = "s3cmd put download_data/ --recursive %s" % self.s3_partition_dir
        s3_copy_flag = runCmd(s3_copy_cmd)
        if s3_copy_flag:
            print "Moved all data files for current run to S3 bucket"
        if not s3_copy_flag:
            print "FAILURE: %s" % s3_copy_cmd
            exit(1)
        return True

    def cleanup(self):
        """
        Cleanup S3 data directory for redundant partitions.
        This replicates inset overwrite function of Hive
        """
        s3_partition_pattern = "%sdata/*" % (self.s3_dir_path)
        list_par = glob.glob(s3_partition_pattern)
        if len(list_par) > 0:
            print "List of partitions to be reomved: %s" % list_par
            for partition in list_par:
                rmv_cmd = "s3cmd rm --recursive %s" % partition
                run_flag = runCmd(rmv_cmd)
                if not run_flag:
                    print "FAILURE: %s" % rmv_cmd
                    exit(1)
            print "Successfully removed all the redundant partitions"
        else:
            print "No Cleanup required. This is a fresh run"

    def cleanMeta(self):
        """
        This function cleans up the Meta dir
        """
        s3_meta_dir = "%smeta" % self.s3_dir_path
        rmv_cmd = "s3cmd rm %s --recursive" % s3_meta_dir
        run_flag = runCmd(rmv_cmd)
        if not run_flag:
            print "FAILURE: %s" % rmv_cmd
            exit(1)
        else:
            print "SUCCESS: All Meta for FBInsights removed."

    def readParams(self):
        """
        This function reads parameters into param_values dict
        """
        print ("Get parameters: ")
        self.environment = 'test'
        self.run_id = '20190402035102'
        self.git_ver = '300fa20a5ce9a7d05c2ed95bcad15685981ba5b2-EDES_798'

        self.param_values['FB_ACCESS_TOKEN'] = 'EAABZBZBbPfTMgBAJCgKVZBYHhjxgAHfFZCmTBbW3pWtJTJmef1KNx96IqZC9WU3qeyZC6v1SbQ7PtqMYQQCcmvIZCZABHaozXUZB3i5uWI0DGd5SvdCnjiwGQPSc25LIUYLFYRstUg70ELKvdAd5oHo17iJvwJhry0l6GZCUdmZCJUMwIag3KLdXZCK35z5kTtRt5Otjoac5WasUFQZDZD'
        self.param_values['BUSINESS_ID'] = '10152208907207680'
        self.param_values['API_URL_PREFIX'] = 'https://graph.facebook.com'
        self.param_values['API_VERSION'] = 'v3.1'
        inp_acct_id_api_comp = 'endpoint:owned_ad_accounts,client_ad_accounts;field:id,name;limit:300'
        inp_acct_id_api_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                   for key_val in inp_acct_id_api_comp.split(';')]))

        self.param_values['ACCT_ID_API_URL_COMPONENTS'] = inp_acct_id_api_comp

        inp_run_id_api_comp = 'sort:sort=ad_id;level:level=ad;increment:time_increment=1;time_range_prefix:time_range={since:;time_range_suffix:,until:'
        inp_run_id_api_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                  for key_val in inp_run_id_api_comp.split(';')]))
        self.param_values['RUN_ID_API_URL_COMPONENTS'] = inp_run_id_api_comp


        inp_acct_access_cmd_comp = 'user:100012771877365;cmd_suffix_post:userpermissions'
        inp_acct_access_cmd_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                       for key_val in inp_acct_access_cmd_comp.split(';')]))
        self.param_values['ACCT_ACCESS_CMD_COMPONENTS'] = inp_acct_access_cmd_comp

        self.param_values['ACCT_ACCESS_CMD_ROLES'] = 'ADMIN,REPORTS_ONLY,GENERAL_USER'

        field_list_1 = 'date_start,date_stop,total_action_value,impressions,clicks'
        field_list_2 = 'unique_clicks,spend,frequency,cost_per_inline_post_engagement,inline_link_clicks,cost_per_inline_link_click'
        field_list_3 = 'inline_post_engagement,unique_inline_link_clicks,cost_per_unique_inline_link_click,inline_link_click_ctr,unique_inline_link_click_ctr,reach,ctr,unique_ctr'
        field_list_4 = 'unique_link_clicks_ctr,cpm,cpp,cost_per_unique_click,estimated_ad_recall_rate,estimated_ad_recall_rate_lower_bound,estimated_ad_recall_rate_upper_bound'
        field_list_5 = 'cost_per_estimated_ad_recallers,canvas_avg_view_time,canvas_avg_view_percent,ad_id,adset_id,campaign_id,account_id,relevance_score'

        field_list_insight_1 = 'date_start,date_stop,total_actions,total_unique_actions,total_action_value,impressions,social_impressions,clicks,social_clicks'
        field_list_insight_2 = 'unique_impressions,unique_social_impressions,unique_clicks,unique_social_clicks,spend,frequency,deeplink_clicks,app_store_clicks,website_clicks,cost_per_inline_post_engagement'
        field_list_insight_3 = 'inline_link_clicks,cost_per_inline_link_click,inline_post_engagement,unique_inline_link_clicks,cost_per_unique_inline_link_click,inline_link_click_ctr,unique_inline_link_click_ctr'
        field_list_insight_4 = 'call_to_action_clicks,reach,social_reach,ctr,unique_ctr,unique_link_clicks_ctr,cpm,cpp,cost_per_total_action,cost_per_unique_click,estimated_ad_recall_rate,estimated_ad_recall_rate_lower_bound'
        field_list_insight_5 = 'estimated_ad_recall_rate_upper_bound,cost_per_estimated_ad_recallers,canvas_avg_view_time,canvas_avg_view_percent,ad_id,adset_id,campaign_id,account_id,relevance_score'

        field_list = "%s,%s,%s,%s,%s" % \
                     (field_list_1, field_list_2, field_list_3, field_list_4, field_list_5)
        self.param_values['FIELD_LIST'] = field_list.replace('\r', '')
        field_list_insight = "%s,%s,%s,%s,%s" % \
                             (field_list_insight_1, field_list_insight_2, field_list_insight_3,
                              field_list_insight_4, field_list_insight_5)
        self.param_values['FIELD_LIST_INSIGHT'] = field_list_insight.replace('\r', '')

        dwnld_acct_id_api_url_comp = 'endpoint:insights;limit:100'
        dwnld_acct_id_api_url_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                         for key_val in dwnld_acct_id_api_url_comp.split(';')]))
        self.param_values['DWNLD_ACCT_ID_API_URL_COMPONENTS'] = dwnld_acct_id_api_url_comp

        self.param_values['MAX_ETL_LOAD_DATE'] = '2019-04-14'
        self.param_values['INSIGHTS_RESTATEMENT'] = None
        self.param_values['RESTATEMENT_START_DATE'] = None
        self.param_values['RESTATEMENT_END_DATE'] = None
        self.param_values['EXCEPTION_INSIGHTS_COLUMN_LIST'] = {'exception_column_name':'default_value','canvas_avg_view_time':'0','canvas_avg_view_percent':'0.0','relevance_score':{'score': '0'},'unique_impressions':'0','unique_social_impressions':'0'
,'deeplink_clicks':'0','app_store_clicks':'0','website_clicks':'0','total_unique_actions':'0','estimated_ad_recall_rate':'0','estimated_ad_recall_rate_lower_bound':'0'
,'estimated_ad_recall_rate_upper_bound':'0','cost_per_estimated_ad_recallers':'0'
,'cost_per_inline_post_engagement':'0','inline_link_clicks':'0','cost_per_inline_link_click':'0'
,'inline_post_engagement':'0','unique_inline_link_clicks':'0','cost_per_unique_inline_link_click':'0'
,'inline_link_click_ctr':'0','unique_inline_link_click_ctr':'0','call_to_action_clicks':'0'
,'reach':'0','social_reach':'0','ctr':'0','cost_per_unique_click':'0','social_impressions':'0'
,'unique_ctr':'0','unique_link_clicks_ctr':'0','cost_per_total_action':'0'
,'total_actions':'0','social_clicks':'0','unique_social_clicks':'0','total_action_value':'0'}

        print self.param_values

    def getAdAccounts(self):
        """
        This function returns list of ad-accounts for each business id
        """
        # get the max date Master process ran for
        # ---- max_date_cmd = """hadoop fs -lsr %s | awk '{print $NF}' | cut -d "/" -f7 | uniq | cut -d "_" -f4 | sort -r | head -1""" % self.s3_meta_path
        # --- proc = subprocess.Popen(max_date_cmd, stdout=subprocess.PIPE, shell=True)
        # ---- (output, err) = proc.communicate()
        curr_max_date = '2019-04-02'
        print "Current max date: %s" % curr_max_date
        print "SUCCESS:Max date retrieved" if curr_max_date else exit("FAILURE: %s" % curr_max_date)
        # get the valid ad-accounts for each business from Master process
        ad_accts_file = "%slist_ad_accts_%s/list_ad_accts_file.txt" % (self.s3_meta_path, curr_max_date)
        s3_get_cmd = "s3cmd get %s" % ad_accts_file
        '''
        s3_get_flag = runCmd(s3_get_cmd)
        if not s3_get_flag:
            print "Failed to get accounts file from s3."
            print "Code will exit now"
            sys.exit(1)
        '''
        with open('C:/Users/v-jskinderskis/Documents/Facebook_feed/list_ad_accts_file.txt', 'r') as a:
            list_biz_id_ad_accounts = a.read()

        print list_biz_id_ad_accounts
        return list_biz_id_ad_accounts

    def writeBackParams(self, max_load_date, set_param_file):
        """
        Write and Copy default params to S3
        """

        # write restatement reset params
        file = open(set_param_file, 'w+')
        file.write("%s=%s" % ('INSIGHTS_RESTATEMENT', ''))
        file.write("\n%s=%s" % ('RESTATEMENT_START_DATE', ''))
        file.write("\n%s=%s" % ('RESTATEMENT_END_DATE', ''))
        file.write("\n%s=%s" % ('MAX_ETL_LOAD_DATE', max_load_date))
        file.close()

        print "Updating non-restatement Meta parameter to NULL..."
        print "MAX_ETL_DATE - %s" % max_load_date

        # copy max_load_date param file to S3
        s3_meta_copy_cmd = "s3cmd put %s %smeta/%s/" % (set_param_file, self.s3_dir_path, self.run_id)
        s3_meta_copy_flag = runCmd(s3_meta_copy_cmd)
        print "Moved Meta file to S3" if s3_meta_copy_flag else exit("FAILURE: %s" % s3_meta_copy_cmd)

    def runJob(self):
        """
        job run template method
        """
        self.s3_meta_path = "C:/Users/v-jskinderskis/Documents/Facebook_feed/"
        self.s3_dir_path = "C:/Users/v-jskinderskis/Documents/Facebook_feed/"
        set_param_file = "set_insights_param_%s.txt" % self.run_id
        add_partiton_file = "fb_insights_add_partition_%s.hql" % self.run_id
        # Cleanup redundant partitions in s3 data dir
        # self.cleanup()
        # """
        # rmv_cmd = "s3cmd rm --recursive %sdata/" % self.s3_dir_path
        # run_flag = runCmd(rmv_cmd)
        # print "Successfully removed all the redundant partitions" if run_flag else exit("FAILURE: %s" % rmv_cmd)
        # """
        # self.cleanMeta()

        print "git version: %s" % self.git_ver
        print "environment: %s" % self.environment

        list_biz_id_ad_accounts = self.getAdAccounts()
        # print list_biz_id_ad_accounts
        self.dict_biz_id_ad_accounts = eval(list_biz_id_ad_accounts)
        print self.dict_biz_id_ad_accounts
        self.fb_access_token = self.getFBAccessToken()

        insights_restatement = self.getInsightsRestatement()
        if insights_restatement:
            business_id_list = insights_restatement.split(',')
            print "Processing for Insights Restatement for id's %s" % business_id_list
            self.v_start_date = self.getRestatementStartDate()
            self.v_end_date = self.getRestatementEndDate()
            print "Between [%s] and [%s]" % (self.v_start_date, self.v_end_date)
            if ((not self.v_start_date) or \
                    (not self.v_end_date) or \
                    (datetime.datetime.strptime(self.v_start_date, "%Y-%m-%d").date() > datetime.datetime.strptime(
                        self.v_end_date, "%Y-%m-%d").date())):
                print "Please update Teradata parameters with start date and end date for restatement"
                print "Issue with start and end date"
                print "Start date - %s End date - %s" % (self.v_start_date, self.v_end_date)
                # print "Setting Max load date to backup value"
                # writing back bkp max_load_date to param file
                # self.writeBackParams(self.getMaxETLLoadDate().strftime('%Y-%m-%d'), set_param_file)
                sys.exit(1)

        else:
            print "No restatement specified, pulling up regular AD_ACCOUNT_ID_LIST from facebook API"
            business_ids = self.getBusinessIDList().strip()
            if not business_ids:
                sys.exit("Please update terdata parameters with business id(s)")
            business_id_list = business_ids.split(',')
            print "Processing for ids %s" % business_id_list
            etl_load_date = self.getMaxETLLoadDate()
            if (not etl_load_date):
                self.v_start_date = "2001-01-01"
            else:
                start_date = datetime.datetime.strptime(etl_load_date, "%Y-%m-%d").date()
                self.v_start_date = str(start_date + datetime.timedelta(days=1))
            self.v_end_date = str(datetime.date.today() - datetime.timedelta(days=1))
            if datetime.datetime.strptime(self.v_start_date, "%Y-%m-%d").date() > datetime.datetime.strptime(
                    self.v_end_date, "%Y-%m-%d").date():
                print "Issue with start and end date"
                print "Start date - %s End date - %s" % (self.v_start_date, self.v_end_date)
                print "Workflow has already run for today. Pulled all Insights data upto %s" % self.v_end_date
                print "Issue a restatement if you want to run the workflow again"
                # self.writeBackParams(self.getMaxETLLoadDate(), set_param_file)
                sys.exit(1)
            print "Between [%s] and [%s]" % (self.v_start_date, self.v_end_date)

        for id in business_id_list:
            print "*******************************************************************************"
            print "Processing for Business Id [%s]" % id
            flag_completed = False
            loop_count = 0
            while (not flag_completed) and loop_count < 5:
                self.business_id = id
                loop_count += 1
                print "Main Attempt [%d] for Business Id [%s]" % (loop_count, self.business_id)
                run_flag = self.runProcess()
                if not run_flag:
                    break
                flag_completed = self.checkStatus()

            if (run_flag and flag_completed):
                dl_flag = self.downloadAdAcctFiles()
                if dl_flag:
                    print "Download step complete for business_id %s" % self.business_id
                if not dl_flag:
                    print "Download step failed for business_id %s" % self.business_id

            if ((not run_flag) or (not flag_completed) or (not dl_flag)):
                # self.cleanup()
                print "FAIL: Couldn't finish processing for business_id %s" % self.business_id
                # self.writeBackParams(self.getMaxETLLoadDate().strftime('%Y-%m-%d'), set_param_file)
                sys.exit(1)
            self.s_processed += 1
            print "SUCCESS: Finished processing for business_id %s" % self.business_id
            print "*******************************************************************************"

        print "Downloaded files for all Business Id's. Moving Meta and Data file to S3..."
        # self.removeFileHeaders()
        copy_flag = self.copyFilesToS3()
        if not copy_flag:
            # self.writeBackParams(self.getMaxETLLoadDate().strftime('%Y-%m-%d'), set_param_file)
            sys.exit("Error while copying Insights Account files to S3...")

        # Getting MAX load date
        bkp_max_load_date = self.getMaxETLLoadDate()
        print "Bkp max load date: %s" % bkp_max_load_date
        if self.max_date:
            current_max_date = bkp_max_load_date if datetime.datetime.strptime(bkp_max_load_date,
                                                                               "%Y-%m-%d").date() > self.max_date \
                else self.max_date.strftime("%Y-%m-%d")
        else:
            current_max_date = bkp_max_load_date

        # Write max load date to param file
        print "Setting Restatement parameters back to default values..."
        print "Updating max_load_date parameter:"
        print "MAX_ETL_LOAD_DATE - %s" % current_max_date
        self.writeBackParams(current_max_date, set_param_file)

        # Pushing Partiton refresh param file to S3
        s3_add_partiton_cmd = """ALTER TABLE lz.eww_pm_fb_insights_spend_ad_level_detail ADD IF NOT EXISTS PARTITION (load_date='%s') location '%s' """ % \
                              (self.current_date, self.s3_partition_dir)

        file = open(add_partiton_file, 'w+')
        file.write(s3_add_partiton_cmd)
        file.close()
        s3_meta_copy_cmd = "s3cmd put %s %smeta/%s/" % (add_partiton_file, self.s3_dir_path, self.run_id)
        s3_meta_copy_flag = runCmd(s3_meta_copy_cmd)
        print "Moved Meta file to S3" if s3_meta_copy_flag else exit("FAILURE: %s" % s3_meta_copy_cmd)

        print ""
        print "************************************************"
        print "SUMMARY"
        print "TOTAL BUSINESS ID's: %d" % len(business_id_list)
        print "BUSINESS ID's SUCCESSFULLY PROCESSED: %d" % self.s_processed
        print "TOTAL FILES GENERATED: %d" % self.total_files_generated
        print "************************************************"


if __name__ == '__main__':
    returnValue = -1
    print "Started Facebook Insights Process"
    job = FBInsightsJob()
    job.readParams()
    job.runJob()
    print "Process finished successfully"
    returnValue = 0
    sys.exit(returnValue)

