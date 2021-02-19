from __future__ import print_function
from __future__ import absolute_import


from builtins import str
from builtins import zip
from builtins import object
import requests
import re
from time import sleep
from datetime import datetime
import shutil
import elasticsearch2
from elasticsearch_dsl import Search, Q
from collections import OrderedDict
from sqlalchemy import create_engine
from subprocess import Popen, PIPE, STDOUT
import shlex
import glob
import csv

# from apiclient.discovery import build
from .utils import Filename, FileType, Date, conf, logger, sort
from .report import Report


class Gather(object):
    """gather data from various sources (canonical list, solr, etc.)
    ads files are placed in a while known directory with a name based on date
    and their contents"""

    def __init__(self, date=Date.TODAY):
        """use passed date as prefix in filenames"""
        self.date = date
        self.values = {}
        self.values['passed_tests'] = []
        self.values['failed_tests'] = []

    def all(self):
        self.solr_admin()
        self.canonical()
        self.postgres()
        self.classic()
        self.solr_bibcodes_list()
        try:
            self.errorsearch()
        except Exception as err:
            logger.info('Problem with error searching: %s' % err)
        try:
            self.fulltext()
        except Exception as err:
            logger.info('Problem with fulltext searching: %s' % err)

    def canonical(self):
        """create local copy of canonical bibcodes"""
        c = conf.get('CANONICAL_FILE', '/proj/ads_abstracts/config/bibcodes.list.can')
        air = Filename.get(self.date, FileType.CANONICAL)
        logger.debug('making local copy of canonical bibcodes file, from %s to %s', c, air)
        shutil.copy(c, air)
        sort(air)

    def return_query(self, url):
        try:
            rQuery = requests.get(url)
            if rQuery.status_code != 200:
                print('Nope: %s' % rQuery.status_code)
                return {}
            else:
                return rQuery.json()
        except Exception as err:
            return {}

    def solr_admin(self):
        """obtain admin oriented data from solr instance """
        url_base = conf.get('SOLR_URL', 'http://localhost:9983/solr/collection1/')
        query = 'admin/mbeans?stats=true&cat=%s&wt=json'
        # Default values if solr.mbeans queries fail...
        solr_cumulative_adds = -1
        solr_cumulative_errors = -1
        solr_errors = -1
        solr_deleted = -1
        solr_bibcodes = -1
        solr_indexsize = -1
        solr_indexgen = -1

        url_1 = (url_base + query) % 'REPLICATION'
        url_2 = (url_base + query) % 'CORE'
        url_3 = (url_base + query) % 'UPDATE'

        try:
            j = self.return_query(url_1)
            solr_val = j['solr-mbeans'][1]['/replication']['stats']
            solr_indexsize = solr_val['REPLICATION./replication.indexSize']
            solr_indexgen = solr_val['REPLICATION./replication.generation']
        except Exception as err:
            logger.warn('Error getting REPLICATION data: %s' % err)

        try:
            j = self.return_query(url_2)
            solr_val = j['solr-mbeans'][1]['searcher']['stats']
            solr_deleted = solr_val['SEARCHER.searcher.deletedDocs']
            solr_bibcodes = solr_val['SEARCHER.searcher.numDocs']
        except Exception as err:
            logger.warn('Error getting CORE data: %s' % err)

        try:
            j = self.return_query(url_3)
            solr_val = j['solr-mbeans'][1]['updateHandler']['stats']
            solr_cumulative_adds = solr_val['UPDATE.updateHandler.cumulativeAdds.count']
            solr_cumulative_errors = solr_val['UPDATE.updateHandler.cumulativeErrors.count']
            solr_errors = solr_val['UPDATE.updateHandler.errors']
        except Exception as err:
            logger.warn('Error getting UPDATE data: %s' % err)



        self.values.update({'solr_indexsize': solr_indexsize,
                            'solr_indexgen': solr_indexgen,
                            'solr_deleted': solr_deleted,
                            'solr_bibcodes': solr_bibcodes,
                            'solr_cumulative_adds': solr_cumulative_adds,
                            'solr_cumulative_errors': solr_cumulative_errors,
                            'solr_errors': solr_errors})


    def solr_bibcodes_list(self):
        url = conf.get('SOLR_URL', 'http://localhost:9983/solr/collection1/')
        query_1 = 'select?fl=bibcode&cursorMark='
        query_2 = '&q=*%3A*&rows=20000&sort=bibcode%20asc%2Cid%20asc&wt=json'

        cursormark_token = '*'
        last_token = ''
        bibcode_list = list()
        bibcode_count = 0
        try:
            while cursormark_token != last_token:
                q_url = url + query_1 + cursormark_token + query_2
                rQuery = requests.get(q_url)
                if rQuery.status_code != 200:
                    logger.warn('Can\'t harvest bibcode list from Solr: %s ; %s' % (rQuery.status_code, rQuery.text))
                else:
                    last_token = cursormark_token
                    j = rQuery.json()
                    cursormark_token = j['nextCursorMark']
                    resp = j['response']
                    if bibcode_count == 0:
                        bibcode_count = resp['numFound']
                    docs = resp['docs']
                    bibcode_list.extend(x['bibcode'] for x in docs)
        except Exception as err:
            logger.warn('Failed to extract bibcodes: %s' % err)
        else:
            filename = Filename.get(self.date, FileType.SOLR)
            try:
                with open(filename, 'w') as f:
                    for b in bibcode_list:
                        f.write(b+'\n')
                sort(filename)
            except Exception as err:
                logger.error('In gather.solr_bibcodes_list: %s' % s)


    # def elasticsearch(self):
    def errorsearch(self):
        pipelines = ['master','import','data','fulltext','orcid','citation_capture','augment','myads']

        r = Report(None,None)
        for p in pipelines:
            try:
                logstream = 'fluent-bit-backoffice_prod_%s_pipeline_1' % p
                query = '"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"' + logstream + '\\" +@message:\\"error\\""'
                result = r.query_Kibana(query=query, n_days=1, rows=10000)
                count = result['responses'][0]['hits']['total']
                err_key = p + "_piperr"
                self.values[err_key] = count
            except Exception as err:
                logger.warn('Error finding errors! %s' % err)

        # self.values['fluent-bit-backoffice_fulltext_pipeline_1'] = '123'
        # next, check on specific errors that should have been fixed
        # message must be in double quotes to force exact phrase match
        tests = (('fluent-bit-backoffice_prod_master_pipeline_1', 'too many records to add to db'),
                 ('fluent-bit-backoffice_prod_fulltext_pipeline_1', 'is linked to a non-existent file'),
                 ('fluent-bit-backoffice_prod_augment_pipeline_1', 'Unbalanced Parentheses'))
        passed_tests = []
        failed_tests = []
        for logstream, message in tests:
            try:
                query = '"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"%s\\" +@message:\\"%s\\""' % (logstream, message)
                result = r.query_Kibana(query=query, n_days=1, rows=10000)
                count = result['responses'][0]['hits']['total']
                if count == 0:
                    passed_tests.append('%s, message %s\n' % (logstream, message))
                else:
                    failed_tests.append('Unexpected error in %s: %s occured %s times' % (logstream, message, count))
            except Exception as err:
                logger.warn('Error finding errors! %s' % err)
        errors = {}
        if len(failed_tests):
            errors['failed_tests'] = failed_tests
        if len(passed_tests):
            errors['passed_tests'] = passed_tests
        try:
            logger.info(errors)
        except:
            pass
        self.values['failed_tests'].extend(failed_tests)
        self.values['passed_tests'].extend(passed_tests)

    def classic(self):
        """are there errors from the classic pipeline"""
        files = ('/proj/ads/abstracts/sources/ArXiv/log/update.log',
                 '/proj/ads/abstracts/sources/ArXiv/log/usage.log')
        for f in files:
            self.classic_file_check(f)

    def classic_file_check(self, f):
        x = Popen(['grep', '-i', 'error', f], stdout=PIPE, stderr=STDOUT)
        resp = x.communicate()[0]
        if x.returncode == 1:
            # no errors found in log files
            msg = 'passed arxiv check: file %s' % f
            logger.info(msg)
            self.values['passed_tests'].extend(msg)
        else:
            # return code = 0 if grep matched
            # return code = 2 if grep encounted an error
            # msg = 'failed arxiv check: file {}, error {}'.format(f, resp)
            msg = 'failed arxiv check: file %s, error = \n%s' % (f, resp)
            logger.info(msg)
            self.values['failed_tests'].extend(msg)

    def postgres(self):
        # consider building on ADSPipelineUtils

        engine = create_engine(conf.get('SQLALCHEMY_URL_MASTER', 'postgres://master_pipeline:master_pipeline@localhost:15432/master_pipeline'), echo=False)
        connection = engine.connect()
        self.values['metrics_updated_count'] = self.exec_sql(connection,
                                                        "select count(*) from records where metrics_updated>now() - interval ' 1 day';")
        self.values['metrics_null_count'] = self.exec_sql(connection,
                                                     "select count(*) from records where metrics is null;")

        self.values['master_total_changed'] = self.exec_sql(connection,
                                                       "select count(*) from records where processed >= NOW() - '1 day'::INTERVAL;")
        self.values['master_solr_changed'] = self.exec_sql(connection,
                                                      "select count(*) from records where solr_processed >= NOW() - '1 day'::INTERVAL;")
        self.values['master_bib_changed'] = self.exec_sql(connection,
                                                     "select count(*) from records where bib_data_updated >= NOW() - '1 day'::INTERVAL;")
        self.values['master_fulltext_changed'] = self.exec_sql(connection,
                                                          "select count(*) from records where fulltext_updated >= NOW() - '1 day'::INTERVAL;")
        self.values['master_orcid_changed'] = self.exec_sql(connection,
                                                       "select count(*) from records where orcid_claims_updated >= NOW() - '1 day'::INTERVAL;")
        self.values['master_nonbib_changed'] = self.exec_sql(connection,
                                                        "select count(*) from records where nonbib_data_updated >= NOW() - '1 day'::INTERVAL;")

        connection.close()
        logger.info('from metrics database, null count = %s, 1 day updated count = %s' % (self.values['metrics_null_count'], self.values['metrics_updated_count']))

    def exec_sql(self, connection, query):
        result = connection.execute(query)
        count = result.first()[0]
        return str(count)

    def fulltext(self):

        """Get errors from todays fulltext logs and generate a list for each
        type of error of corresponding bibcodes and source directories. These
        lists are written to files that are further processed in compute.py"""

        # types of errors with corresponding file names
        errors = conf.get('FULLTEXT_ERRORS','')

        # get todays date
        now = datetime.strftime(datetime.now(), "%Y-%m-%d")

        # loop through types of errors messages
        for err_msg in list(errors.keys()):

            bibs = []
            dirs = []

            # location of bibcode and directory in message field
            """example log:
            {"asctime": "2019-08-26T11:38:34.201Z", "msecs": 201.6739845275879,
            "levelname": "ERROR", "process": 13411, "threadName": "MainThread",
            "filename": "checker.py", "lineno": 238, "message": "Bibcode '2019arXiv190105463B'
            is linked to a non-existent file '/some/directory/filename.xml'",
            "timestamp": "2019-08-26T11:38:34.201Z", "hostname": "adsvm05"}"""
            loc_bib = 1
            loc_dir = 3

            if (err_msg == "No such file or directory"):
                loc_bib = 3
                loc_dir = 11
            elif (err_msg == "format not currently supported for extraction"):
                loc_bib = 7
                loc_dir = 23

            # loop through files
            for name in glob.glob(errors[err_msg]):

                command = "awk -F\: '/" + err_msg + "/ && /" + now + "/ && /ERROR/ {print $0}' " + name
                args = shlex.split(command)

                x = Popen(args, stdout=PIPE, stderr=STDOUT)

                # get bibcodes/directories from todays errors
                try:
                    resp = x.communicate()[0].split("\n")

                    for r in resp:
                        if r:
                            r = r.split("'")
                            bibs.append(r[loc_bib])
                            dirs.append(r[loc_dir])
                except Exception as err:
                    logger.debug("Error from gather.fulltext(): %s " % err)

            # create filename based on error message and date
            fname = Filename.get(self.date, FileType.FULLTEXT, adjective=None,
                                 msg="_" + ("_".join(err_msg.split()))
                                 .replace('-', '_') + "_")

            # write bibcodes and directories for each error type to file
            with open(fname, 'w') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerows(list(zip(bibs, dirs)))

            sort(fname)
