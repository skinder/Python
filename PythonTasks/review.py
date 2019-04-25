import json
import urllib2

def importJSON(str_url):
    """
    function to query the API
    """
    print "connecting to url :", str_url[:str_url.index('access_token=')+len("access_token=")]
    try:
        url_response = urllib2.urlopen(str_url, timeout=60)
        #json_data_out = json.loads(url_response.read())
        #return json_data_out
    except urllib2.HTTPError, e:
        print 'HTTPError %s' % str(e.code)
        return
        #raise
    except IOError:
        print "Error while opening the URL"
        return
        #raise
    except urllib2.URLError, e:
        print 'URLError %s' % str(e.reason)
        return
        #raise
    except httplib.HTTPException, e:
        print "HTTPException"
        return
        #raise
    except Exception:
        print 'generic exception: ' + traceback.format_exc()
        return
        #raise
    json_data_out = json.loads(url_response.read())
    return json_data_out




def readParams():

    param_values = dict()
    param_values['FB_ACCESS_TOKEN'] = 'EAABZBZBbPfTMgBAJCgKVZBYHhjxgAHfFZCmTBbW3pWtJTJmef1KNx96IqZC9WU3qeyZC6v1SbQ7PtqMYQQCcmvIZCZABHaozXUZB3i5uWI0DGd5SvdCnjiwGQPSc25LIUYLFYRstUg70ELKvdAd5oHo17iJvwJhry0l6GZCUdmZCJUMwIag3KLdXZCK35z5kTtRt5Otjoac5WasUFQZDZD'
    param_values['BUSINESS_ID'] = '10152208907207680'
    param_values['API_URL_PREFIX'] = 'https://graph.facebook.com'
    param_values['API_VERSION'] = 'v3.1'
    inp_acct_id_api_comp = 'endpoint:owned_ad_accounts,client_ad_accounts;field:id,name;limit:500'
    inp_acct_id_api_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                               for key_val in inp_acct_id_api_comp.split(';')]))

    param_values['ACCT_ID_API_URL_COMPONENTS'] = inp_acct_id_api_comp
    inp_run_id_api_comp = 'sort:sort=ad_id;level:level=ad;increment:time_increment=1;time_range_prefix:time_range={since:;time_range_suffix:,until:'
    inp_run_id_api_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                              for key_val in inp_run_id_api_comp.split(';')]))
    param_values['RUN_ID_API_URL_COMPONENTS'] = inp_run_id_api_comp

    inp_acct_access_cmd_comp = 'user:100012771877365;cmd_suffix_post:userpermissions'
    inp_acct_access_cmd_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                   for key_val in inp_acct_access_cmd_comp.split(';')]))
    param_values['ACCT_ACCESS_CMD_COMPONENTS'] = inp_acct_access_cmd_comp

    param_values['ACCT_ACCESS_CMD_ROLES'] = 'ADMIN,REPORTS_ONLY,GENERAL_USER'
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
    param_values['FIELD_LIST'] = field_list.replace('\r', '')
    field_list_insight = "%s,%s,%s,%s,%s" % \
                         (field_list_insight_1, field_list_insight_2, field_list_insight_3,
                          field_list_insight_4, field_list_insight_5)
    param_values['FIELD_LIST_INSIGHT'] = field_list_insight.replace('\r', '')

    dwnld_acct_id_api_url_comp = 'endpoint:insights;limit:100'
    dwnld_acct_id_api_url_comp = "{%s}" % (','.join(["'%s':'%s'" % (key_val.split(':')[0], key_val.split(':')[1]) \
                                                     for key_val in dwnld_acct_id_api_url_comp.split(';')]))
    param_values['DWNLD_ACCT_ID_API_URL_COMPONENTS'] = dwnld_acct_id_api_url_comp

    param_values['MAX_ETL_LOAD_DATE'] = '2019-03-30'
    param_values['INSIGHTS_RESTATEMENT'] = None
    param_values['RESTATEMENT_START_DATE'] = None
    param_values['RESTATEMENT_END_DATE'] = None
    param_values['EXCEPTION_INSIGHTS_COLUMN_LIST'] = None

    print param_values


if __name__ == '__main__':
    returnValue = -1
    print "Started Facebook Insights Process"
    #job = FBInsightsJob()
    readParams()
    #job.runJob()
    #print "Process finished successfully"
    #returnValue = 0
    #sys.exit(returnValue)

    download_acct_id_api = 'https://graph.facebook.com/v3.1/2380686731963834/insights?limit=100&fields=date_start,date_stop,total_action_value,impressions,clicks,unique_clicks,spend,frequency,cost_per_inline_post_engagement,inline_link_clicks,cost_per_inline_link_click,inline_post_engagement,unique_inline_link_clicks,cost_per_unique_inline_link_click,inline_link_click_ctr,unique_inline_link_click_ctr,reach,ctr,unique_ctr,unique_link_clicks_ctr,cpm,cpp,cost_per_unique_click,estimated_ad_recall_rate,estimated_ad_recall_rate_lower_bound,estimated_ad_recall_rate_upper_bound,cost_per_estimated_ad_recallers,canvas_avg_view_time,canvas_avg_view_percent,ad_id,adset_id,campaign_id,account_id,relevance_score&access_token=EAABZBZBbPfTMgBAJCgKVZBYHhjxgAHfFZCmTBbW3pWtJTJmef1KNx96IqZC9WU3qeyZC6v1SbQ7PtqMYQQCcmvIZCZABHaozXUZB3i5uWI0DGd5SvdCnjiwGQPSc25LIUYLFYRstUg70ELKvdAd5oHo17iJvwJhry0l6GZCUdmZCJUMwIag3KLdXZCK35z5kTtRt5Otjoac5WasUFQZDZD'
    json_data = importJSON(download_acct_id_api)
    if 'data' in json_data:
        print (json_data["data"])

    print ("Done")
