from builtins import object
from datetime import datetime, timedelta
from os import remove
import os.path
import urllib3
import requests
from shutil import move
import subprocess
from .exceptions import *

from adsputils import setup_logging, load_config
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from dateutil.relativedelta import relativedelta as tsdelta
from grapi.grapi import Grapi

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
conf = load_config(proj_home=proj_home)
logger = setup_logging('AutomatedIngestReport',
                       level=conf.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=conf.get('LOG_STDOUT', False))

class SlackPublisher(object):
    def __init__(self, mesg):
        self.url = conf.get('SLACK_URL','')
        self.uploadurl = conf.get('GOOGLE_URL_BASE', '')
        self.msg = mesg
        self.header = {'content-type': 'application/json'}

    def push(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        text = self.uploadurl + self.msg
        data = "{'reporturl': '%s'}" % text
        try:
            rQuery = requests.post(self.url, data=data, headers=self.header, verify=False)
        except Exception as err:
            raise SlackPushError(err)
        else:
            if rQuery.status_code != 200:
                logger.warn('Slack notification failed -- status code: %s' % rQuery.status_code)
            else:
                logger.info('URL posted to slack: %s' % text)


# enums used to to generate file names
class FileType(object):
    CANONICAL = 'CANONICAL'
    SOLR = 'SOLR'
    FULLTEXT = 'FULLTEXT'


class FileAdjective(object):
    MISSING = 'MISSING'
    DELETED = 'DELETED'
    EXTRA = 'EXTRA'
    NEW = 'NEW'


class Date(object):
    TODAY = 1
    YESTERDAY = 2


class Filename(object):

    @staticmethod
    def get(_date, _type, adjective=None, msg=None):
        """convert passed date, type and adjective into the proper filename"""
        if _date is Date.TODAY:
            _date = datetime.now()
        elif _date is Date.YESTERDAY:
            _date = datetime.now() - timedelta(days=1)
        elif type(_date).__name__ != 'datetime':
            raise ValueError('invalid date passed {}, expected datetime or valid string'.format(_date))

        d = _date.strftime('%Y%m%d')
        if msg and adjective:
            filename = d + msg.lower() + adjective.lower() + _type.capitalize() + '.txt'
        elif msg:
            filename = d + msg.lower() + _type.capitalize() + '.txt'
        elif adjective:
            filename = d + adjective.lower() + _type.capitalize() + '.txt'
        else:
            filename = d + _type.capitalize() + '.txt'
        data_dir = conf.get('AIR_DATA_DIRECTORY',os.path.join(proj_home, './data'))
        return data_dir + filename


def lines_in_file(filename):
    lines = 0
    try:
        for line in open(filename):
            lines += 1
    except Exception as err:
        logger.error('In utils.lines_in_file: %s' % err)
    return lines


def lines_in_file_foo(filename):
    """based on https://gist.github.com/zed/0ac760859e614cd03652#file-gistfile1-py-L41
       works on mac, does not work on unix
    """
    if os.path.isfile(filename) is False:
        raise ValueError('passed file does not exist')

    out = subprocess.Popen(['wc', '-l', filename],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    # out has a value like
    # ('', ' ', '      1 /app/air/tests/stubdata/20000102missingSolr.txt\n')
    count = int(out.partition(' ')[2].lstrip().split(' ')[0])
    return count


def comm(file_in1, file_in2, file_out):
    """run unix comm command to generated what's in the first file but not the second"""
    for f in [file_in1,file_in2]:
        sorter = 'sort ' + f + ' -o ' + f
        sr = subprocess.call(sorter, shell=True)

    c = 'comm' + ' -2 -3 ' + file_in1 + ' ' + file_in2 + ' > ' + file_out
    r = subprocess.call(c, shell=True)
    if r != 0:
        logger.error('c command returned {}'.format(c, r))
    lines = lines_in_file(file_out)
    return lines


def sort(filename):
    """use temp file and unix sort command essentially sort in place"""

    tmp_filename = '{}.tmp'.format(filename)
    move(filename, tmp_filename)
    c = 'sort {} > {}'.format(tmp_filename, filename)
    r = subprocess.call(c, shell=True)
    if r != 0:
        logger.error('in sort, c command returned {}'.format(c, r))
    remove(tmp_filename)


def _query_graylog(api, query, start, end, fields):
    try:

        q_payload = {"query": query,
                     "from": start,
                     "to": end,
                     "offset": 0,
                     "limit": 1,
                     "fields": fields,
                     "sort": "timestamp:asc"}

        request = api.send("get", **q_payload)
        request.raise_for_status()
        return request.json()
    except Exception as err:
        raise GraylogQueryException("Error querying Graylog: %s" %err)


def query_graylog_all():

    graylog_token = conf.get("GRAYLOG_TOKEN", "")
    graylog_url = conf.get("GRAYLOG_URL", "")

    api = Grapi(url, graylog_token)

    # time range to be searched in Graylog
    now = datetime.utcnow()
    before = now - tsdelta(days=1)
    start = before.isoformat(sep=' ', timespec='milliseconds')
    end = (now + tsdelta(minutes=1)).isoformat(sep=' ', timespec='milliseconds')

    # global query return values
    fields = ",".join(["timestamp", "namespace_name", "container_name", "message"])

    # this is where the results will be stored
    results = {}

    try:
        # query for myads emails
        query_myads = 'namespace_name:back-prod AND container_name:myads_pipeline AND message:"Email sent to *"'
        data = _query_graylog(api, query_myads, start, end, fields)
        results.update({"myads_email_count": data.get("total_results", 0)})

        # queries for specific errors: master, fulltext, augments
        # query for master/resolver errors
        query_resolver_err = 'namespace_name:back-prod AND container_name:master_pipeline AND message:"error sending links"'
        data = _query_graylog(api, query_resolver_err, start, end, fields)
        results.update({"resolver_err_count": data.get("total_results", 0)})

        #   master: too many records
        query_master_too_many_bibcodes = 'namespace_name:back-prod AND container_name:master_pipeline AND message:"too many records to add to db"'
        data = _query_graylog(api, query_resolver_err, start, end, fields)
        results.update({"master_too_many_bibs": data.get("total_results", 0)})

        #   fulltext: missing files
        query_fulltext_non_existent_files = 'namespace_name:back-prod AND container_name:fulltext_pipeline AND message:"is linked to a non-existent file"'
        data = _query_graylog(api, query_resolver_err, start, end, fields)
        results.update({"fulltext_nonexistent_file": data.get("total_results", 0)})

        #   augments: unmatched parentheses
        query_fulltext_non_existent_files = 'namespace_name:back-prod AND container_name:augment_pipeline AND message:"Unbalanced Parentheses"'
        data = _query_graylog(api, query_resolver_err, start, end, fields)
        results.update({"augments_unbalanced_parens": data.get("total_results", 0)})

        # check pipelines for any errors
        pipelines = ['master','import','data','fulltext','orcid','citation_capture','augment','myads']
        # query for general errors
        query_pipeline_err = 'namespace_name:back-prod AND container_name:%s_pipeline AND message:"error"'

        for p in pipelines:
            key = "%s_piperr" % p
            q = query_pipeline_err % p
            data = _query_graylog(api, q, start, end, fields)
            results.update({key: data.get("total_results", 0)})

    except Exception as err:
        raise GraylogException("Error querying graylog: %s" % err)
    else:
        return results

