from __future__ import print_function
from __future__ import absolute_import

from builtins import str
from builtins import object
from dateutil.tz import tzutc
import datetime
import time
import pytz
import urllib3
import requests

# from apiclient.discovery import build
# from oauth2client.file import Storage
from string import Template
from .utils import Date, conf, logger

# this code is not yet complete


class Report(object):
    """create ingest report based on data that has been gathered and computed

    Reports (eventually) are google docs, they are created using google's python api
    """

    def __init__(self, gather, compute, date=Date.TODAY):
        # service = build('drive', 'v3', developerKey='')
        self.gather = gather
        self.compute = compute

    def _upload_files(self):
        """upload all the files that the report will link to, return  dict of file name to google file id"""
        pass

    def _upload_file(self, filename, text):
        """upload passed file to default location in google doc, return google's file id

        create json object required by google api and send to google drive
        based on https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/drive_v3.files.html#create
        """
        j = {'starred': True,
             'name': filename}

    def exists(self, filename):
        """return the google file id  if the file exists on google drive"""
        pass

    def _text(self):
        """return text for report, including links"""
        d = {}
        d.update(self.gather.values)
        d.update(self.compute.values)
        t = Template(self._text_template).safe_substitute(d)
        return t

    def _html(self):
        """return an html representation of the report"""

    def query_Kibana(self, query='"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_myads_pipeline_1\\" +@message:\\"Email sent to\\""',
                     n_days=1, rows=1):
        """
        Function to query Kibana for a given input query and return the response.

        :param query: string query, same as would be entered in the Kibana search input (be sure to escape quotes and wrap
            query in double quotes - see default query for formatting)
        :param n_days: number of days backwards to query, starting now (=0 for all time)
        :param rows: number of results to return. If you just need the total number of hits and not the results
            themselves, can be small.
        :return: JSON results
        """

        # get start and end timestamps (in milliseconds since 1970 epoch)
        now = datetime.datetime.now(tzutc())
        epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
        end_time = (now - epoch).total_seconds() * 1000.
        if n_days != 0:
            start_time = (now - datetime.timedelta(days=n_days) - epoch).total_seconds() * 1000.
        else:
            midnight = datetime.datetime.combine(now, datetime.time.min).replace(tzinfo=pytz.UTC)
            start_time = ((midnight - epoch).total_seconds() - (5.*3600.)) * 1000.
            # start_time = 0.
        start_time = str(int(start_time))
        end_time = str(int(end_time))

        q_rows = '{"index":["cwl-*"]}\n{"size":%.0f,"sort":[{"@timestamp":{"order":"desc","unmapped_type":"boolean"}}],' % (rows)

        q_query = '"query":{"bool":{"must":[{"query_string":{"analyze_wildcard":true, "query":'+query+'}}, '

        q_range = '{"range": {"@timestamp": {"gte": %s, "lte": %s,"format": "epoch_millis"}}}], "must_not":[]}}, ' % (start_time, end_time)

        q_doc = '"docvalue_fields":["@timestamp"]}\n\n'

        data = (q_rows + q_query + q_range + q_doc)

        header = {'origin': 'https://pipeline-kibana.kube.adslabs.org',
                  'authorization': 'Basic ' + conf.get('KIBANA_TOKEN',''),
                  'content-type': 'application/x-ndjson',
                  'kbn-version': '5.5.2'}

        url = 'https://pipeline-kibana.kube.adslabs.org/_plugin/kibana/elasticsearch/_msearch'

        # set to bypass SSL cert problem w/ Kibana
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        resp = requests.post(url, data=data, headers=header, verify=False)

        if resp.status_code == 200:
            results = resp.json()
            return results
        else:
            logger.warn('For query {}, there was a network problem: {0}\n'.format(query,resp))
            return None

    def kibana_counter(self):
        for k, v in conf.get('KIBANA_QUERIES',dict()).items():
            try:
                result = self.query_Kibana(query=k,
                                           n_days=0,
                                           rows=5)
                count = result['responses'][0]['hits']['total']
                logger.info(v % count)
            except Exception as err:
                logger.warning('Unable to execute kibana_counter: %s' % err)
        return

    def create(self):
        pass

    _text_template = '''
    Canonical bibcodes since yesterday: $new_canonical created, $deleted_canonical deleted.

    Solr bibcodes since yesterday: $new_solr new, $deleted_solr deleted.  $missing_solr missing.

    Solr has $solr_cumulative_adds cumulative adds
    and $solr_errors errors
    and $solr_cumulative_errors cumulative_errors.

    Cause Of Solr Changes
    Total number of records changed: $master_total_changed
    Changes sent to solr: $master_solr_changed
    Changes from bib: $master_bib_changed
    Changes from fulltext: $master_fulltext_changed
    Changes from orcid: $master_orcid_changed
    changes from nonbib: $master_nonbib_changed

    Error counts from kibana:
    backoffice_prod_master_pipeline: $master_piperr
    backoffice_prod_import_pipeline: $import_piperr
    backoffice_prod_data_pipeline: $data_piperr
    backoffice_prod_fulltext_pipeline: $fulltext_piperr
    backoffice_prod_orcid_pipeline: $orcid_piperr
    backoffice_prod_citation_capture_pipeline: $citation_capture_piperr
    backoffice_prod_augment_pipeline: $augment_piperr
    backoffice_prod_myads_pipeline: $myads_piperr

    Fulltext Error Counts:
    'extraction failed for bibcode' errors since yesterday: $new_ft_extraction_failed_for_bibcode created, $deleted_ft_extraction_failed_for_bibcode deleted.
    'format not currently supported for extraction' errors since yesterday: $new_ft_format_not_currently_supported_for_extraction created, $deleted_ft_format_not_currently_supported_for_extraction deleted.
    'is linked to a non-existent file' errors since yesterday: $new_ft_is_linked_to_a_non_existent_file created, $deleted_ft_is_linked_to_a_non_existent_file deleted.
    'is linked to a zero byte size file' errors since yesterday: $new_ft_is_linked_to_a_zero_byte_size_file created, $deleted_ft_is_linked_to_a_zero_byte_size_file deleted.
    'No such file or directory' errors since yesterday: $new_ft_No_such_file_or_directory created, $deleted_ft_No_such_file_or_directory deleted.

    nonbib ned row (count should not be zero): $nonbib_ned_row_count

    Metrics info:
    Number of null records = $metrics_null_count
    Number of updates since yesterday = $metrics_updated_count


    '''

    _html_template = '''
    <html>
      <body>

      </body>
    </html>
    '''

    # https://developers.google.com/drive/v3/web/quickstart/python
    # https://developers.google.com/drive/v3/web/simple-upload
    # https://developers.google.com/api-client-library/python/apis/script/v1
    # https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
