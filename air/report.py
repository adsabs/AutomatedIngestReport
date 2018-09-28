
#from apiclient.discovery import build
#from oauth2client.file import Storage
from string import Template
from utils import Date

# this code is not yet complete


class Report:
    """create ingest report based on data that has been gathered and computed

    Reports (eventually) are google docs, they are created using google's python api
    """

    def __init__(self, gather, compute, date=Date.TODAY):
        #service = build('drive', 'v3', developerKey='')
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

        print t
        return t

    def _html(self):
        """return an html representation of the report"""
        

    def create(self):
        pass

    _text_template = '''
    Canonical bibcodes since yesterday: $new_canonical created, $deleted_canonical deleted.

    Solr bibcodes since yesterday: $new_solr new, $deleted_solr deleted.  $missing_solr missing.

    Solr has $solr_cumulative_adds) cumulative adds
    and $solr_errors errors
    and $solr_cumulative_errors cumulative_errors.

    Cause Of Solr Changes
    Total number of records changed: $master_total_changed
    Changes sent to solr: $master_solr_changed
    Changes from bib: $master_bib_changed
    Changes from fulltext: $master_fulltext_changed
    Changes from orcid: $master_orcid_changed
    changes from nonbib: $master_nonbib_changed

    Error counts from elasticsearch:
    backoffice-master_pipeline: $backoffice-master_pipeline
    backoffice-import_pipeline: $backoffice-import_pipeline
    backoffice-data_pipeline: $backoffice-data_pipeline
    backoffice-fulltext_pipeline: $backoffice-fulltext_pipeline
    backoffice-orcid_pipeline: $backoffice-orcid_pipeline
    backoffice-citation_capture_pipeline: $backoffice-citation_capture_pipeline

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
