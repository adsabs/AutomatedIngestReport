
#from apiclient.discovery import build
#from oauth2client.file import Storage
from utils import Date

# this code is not yet complete


class Report:
    """create ingest report based on data that has been gathered and computed

    Reports (eventually) are google docs, they are created using google's python api
    """

    def __init__(self, compute, date=Date.TODAY):
        #service = build('drive', 'v3', developerKey='')
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
        t = 'There are new ' + str(self.compute.new_canonical) + ' canonical bibcodes.  \n' \
            + str(self.compute.deleted_canonical) + ' canonical bibcodes were deleted.'
        t += '\n'
        t += 'Solr has ' + str(self.compute.solr) + ' bibcodes.  It has '  \
            + str(self.compute.new_solr) + ' new bibcodes, ' \
            + str(self.compute.deleted_solr) + ' bibcodes were deleted.' \
            + str(self.compute.missing_solr) + ' canonical bibcodes are missing.'
        print t

    def create(self):
        pass

    # https://developers.google.com/drive/v3/web/quickstart/python
    # https://developers.google.com/drive/v3/web/simple-upload
    # https://developers.google.com/api-client-library/python/apis/script/v1
    # https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
