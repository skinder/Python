#!/usr/bin/python

# title           :fb_udfs.py
# description     :This script has udf's used by fb PM scripts
# author          :prshah
# date            :20170401
# version         :1.1
# python_version  :2.6.6
# ==============================================================================
import os, subprocess
import urllib2
import json
import traceback
import httplib


def importJSON(str_url):
    """
    function to query the API
    """
    print "connecting to url :", str_url[:str_url.index('access_token=') + len("access_token=")]
    try:
        url_response = urllib2.urlopen(str_url, timeout=60)
        # json_data_out = json.loads(url_response.read())
        # return json_data_out
    except urllib2.HTTPError, e:
        print 'HTTPError %s' % str(e.code)
        return
        # raise
    except IOError:
        print "Error while opening the URL"
        return
        # raise
    except urllib2.URLError, e:
        print 'URLError %s' % str(e.reason)
        return
        # raise
    except httplib.HTTPException, e:
        print "HTTPException"
        return
        # raise
    except Exception:
        print 'generic exception: ' + traceback.format_exc()
        return
        # raise
    json_data_out = json.loads(url_response.read())
    return json_data_out


def postReqWithJSONResponse(str_url, str_data):
    """
    function to return JSON with str_url and str_data
    """
    print "connecting to url : %s?%s" % (str_url, str_data)
    try:
        req = urllib2.Request(str_url, str_data)
        url_response = urllib2.urlopen(req, timeout=60)
    except urllib2.HTTPError, e:
        print 'HTTPError %s' % str(e.code)
        return
        # raise
    except IOError:
        print "Error while opening the URL"
        return
        # raise
    except urllib2.URLError, e:
        print 'URLError %s' % str(e.reason)
        return
        # raise
    except httplib.HTTPException, e:
        print "HTTPException"
        return
        # raise
    except Exception:
        print 'generic exception: ' + traceback.format_exc()
        return
        # raise
    json_data_out = json.loads(url_response.read())
    return json_data_out


def runCmd(cmd):
    print "executing command: %s " % cmd
    try:
        result_cmd = subprocess.call(cmd, shell=True)
        if result_cmd == 0:
            # print "Command successfully completed %s" % cmd
            return True
        else:
            # print "Error running command %s" % cmd
            return False
    except:
        print "Unkown OS Error while executing command %s" % cmd

