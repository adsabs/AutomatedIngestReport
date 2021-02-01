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
        jobid = self.solr_bibcodes_start()
        logger.info('Solr bibcodes JobID: %s' % jobid)
        self.canonical()
        try:
            self.elasticsearch()
        except Exception as err:
            logger.debug('Error from gather.elasticsearch(): %s' % err)
        self.postgres()
        self.classic()
        self.solr_bibcodes_finish(jobid)
        self.fulltext()

    def canonical(self):
        """create local copy of canonical bibcodes"""
        c = conf['CANONICAL_FILE']
        air = Filename.get(self.date, FileType.CANONICAL)
        logger.info('making local copy of canonical bibcodes file, from %s to %s', c, air)
        shutil.copy(c, air)
        sort(air)

    def solr(self):
        self.solr_admin()
        self.solr_bibcodes()

    def solr_admin(self):
        """obtain admin oriented data from solr instance """
        url = conf.get('SOLR_URL', 'http://localhost:9983/solr/collection1/')
        query = 'admin/mbeans?stats=true&cat=UPDATEHANDLER&wt=json'
        rQuery = requests.get(url + query)
        if rQuery.status_code != 200:
            logger.error('failed to obtain stats on update handler, status code = %s', rQuery.status_code)
        else:
            j = rQuery.json()
            self.values['solr_cumulative_adds'] = j['solr-mbeans'][1]['updateHandler']['stats']['cumulative_adds']
            self.values['solr_cumulative_errors'] = j['solr-mbeans'][1]['updateHandler']['stats']['cumulative_errors']
            self.values['solr_errors'] = j['solr-mbeans'][1]['updateHandler']['stats']['errors']

    def solr_bibcodes(self):
        jobid = self.solr_bibcodes_start()
        self.solr_bibcodes_finish(jobid)

    def solr_bibcodes_start(self):
        """use solr batch api to get list of all bibcode it has

        based on http://labs.adsabs.harvard.edu/trac/adsabs/wiki/SearchEngineBatch#Example4:Dumpdocumetsbyquery"""
        url = conf.get('SOLR_URL', 'http://localhost:9983/solr/collection1/')
        query = 'batch?command=dump-docs-by-query&q=*:*&fl=bibcode&wt=json'
        # use for testing
        # query = 'batch?command=dump-docs-by-query&q=bibcode:2003ASPC..295..361M&fl=bibcode&wt=json'
        start = 'batch?command=start&wt=json'

        logger.info('sending initial batch query to solr at %s', url)
        rQuery = requests.get(url + query)
        logger.info('in solr_bibcodes_start: requests.get returned rQuery object')
        if rQuery.status_code != 200:
            logger.error('initial batch solr query failed, status: %s, text: %s',
                         rQuery.status_code, rQuery.text)
            return False
        else:
            logger.info('solr bibcodes: rQuery returned status=200')
        j = rQuery.json()
        jobid = j['jobid']
        logger.info('sending solr start batch command')
        rStart = requests.get(url + start)
        if rStart.status_code != 200:
            logger.error('solr start batch processing failed, status %s, text: %s',
                         rStart.status_code, rStart.text)
            return False
        else:
            logger.info('solr bibcodes: rStart returned status=200')

        return jobid

    def solr_bibcodes_finish(self, jobid):
        """get results from earlier submitted job"""
        url = conf.get('SOLR_URL', 'http://localhost:9983/solr/collection1/')
        status = 'batch?command=status&wt=json&jobid='
        get_results = 'batch?command=get-results&wt=json&jobid='
        # now we wait for solr to process batch query
        finished = False
        startTime = datetime.now()
        logger.info('Starting Solr bibcode fetch at %s' % startTime.strftime('%c'))
        while not finished:
            rStatus = requests.get(url + status + jobid)
            if rStatus.status_code != 200:
                logger.error('batch status check failed, status: %s, text: %s',
                             rStatus.status_code, rStatus.text)
                return False
            j = rStatus.json()
            if j['job-status'] == 'finished':
                finished = True
            else:
                sleep(10)
            if (datetime.now() - startTime).total_seconds() > 3600 * 3:
                logger.error('solr batch process taking too long, seconds: %s;',
                             (datetime.now() - startTime).total_seconds())
                return False
            else:
                elapsed = (datetime.now() - startTime).total_seconds()
                isec = int(0.5 + (elapsed/600.))
                if isec % 600 == 0:
                    logger.debug('solr batch check in: still running, %s sec' % elapsed)

        logger.info('solr batch completed in %s seconds, now fetching bibcodes',
                    (datetime.now() - startTime).total_seconds())
        rResults = requests.get(url + get_results + jobid)
        if rResults.status_code != 200:
            logger.error('failed to obtain bibcodes from solr batch query, status: %s, text: %s,',
                         rResults.status_code, rResults.text)
            return False

        # finally save bibcodes to file
        bibs = rResults.text  # all 12 million bibcodes are in this one text field
        # convert to json-ish text to simple string, response includes newlines between bibcodes
        bibs = re.sub(r'{"bibcode":"|,|"}', '', bibs)
        filename = Filename.get(self.date, FileType.SOLR)
        try:
            with open(filename, 'w') as f:
                f.write(bibs)
            sort(filename)
        except Exception as err:
            logger.error('In gather.solr_bibcodes_finish: %s' % s)

        return True

    def elasticsearch(self):
        """obtain error counts from elasticsearch """
        u = conf['ELASTICSEARCH_URL']
        es = elasticsearch2.Elasticsearch(u)
        # first get total errors for last 24 hours
        s = Search(using=es, index='_all') \
                   .query('match', **{'@message': 'error'}) \
                   .filter('range', **{'@timestamp': {'gte': 'now-24h',
                                                      'lt': 'now'}}).count()
        errors = OrderedDict()  # using ordered dict to control order in report
        errors['total'] = s
        # now get errors individually for each pipeline
        pipelines = ('backoffice-master_pipeline',
                     'backoffice-import_pipeline',
                     'backoffice-data_pipeline',
                     'backoffice-fulltext_pipeline',
                     'backoffice-orcid_pipeline',
                     'backoffice-citation_capture_pipeline')
        for pipeline in pipelines:
            s = Search(using=es, index='_all') \
                              .filter('range', **{'@timestamp': {'gte': 'now-24h', 'lt': 'now'}}) \
                              .query('match', **{'@message': 'error'}) \
                              .filter('match', **{'_type': pipeline}) \
                              .count()
            self.values[pipeline] = s
        self.values['backoffice-fulltext_pipeline'] = '123'
        # next, check on specific errors that should have been fixed
        # message must be in double quotes to force exact phrase match
        tests = (('backoffice-master_pipeline', '"too many records to add to db"'),
                 ('backoffice-fulltext_pipeline', '"is linked to a non-existent file"'),
                 ('backoffice-nonbib_pipeline', '"Unbalanced Parentheses"'))
        passed_tests = []
        failed_tests = []
        for pipeline, message in tests:
            count = Search(using=es, index='_all') \
                              .filter('range', **{'@timestamp': {'gte': 'now-24h', 'lt': 'now'}}) \
                              .query('query_string', query=message) \
                              .filter('match', **{'_type': pipeline}) \
                              .count()
            if count == 0:
                passed_tests.append('{}, message {}\n'.format(pipeline, message))
            else:
                failed_tests.append('Unexpected error in {}: {} occured {} times'
                                     .format(pipeline, message, count))
        if len(failed_tests):
            errors['failed_tests'] = failed_tests
        if len(passed_tests):
            errors['passed_tests'] = passed_tests
        print(errors)
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
            msg = 'passed arxiv check: file {}'.format(f)
            print(msg)
            self.values['passed_tests'].extend(msg)
        else:
            # return code = 0 if grep matched
            # return code = 2 if grep encounted an error
            msg = 'failed arxiv check: file {}, error {}'.format(f, resp)
            msg = 'failed arxiv check: file {}, error = \n{}'.format(f, resp)
            print(msg)
            self.values['failed_tests'].extend(msg)

    def postgres(self):
        # consider building on ADSPipelineUtils
        engine = create_engine(conf['SQLALCHEMY_URL_NONBIB'], echo=False)
        connection = engine.connect()
        self.values['nonbib_ned_row_count'] = self.exec_sql(connection, "select count(*) from nonbib.ned;")
        print('from nonbib database, ned table has {} rows'.format(self.values['nonbib_ned_row_count']))
        connection.close()

        engine = create_engine(conf['SQLALCHEMY_URL_MASTER'], echo=False)
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
        print('from metrics database, null count = {}, 1 day updated count = {}'.format(self.values['metrics_null_count'], self.values['metrics_updated_count']))

    def exec_sql(self, connection, query):
        result = connection.execute(query)
        count = result.first()[0]
        return str(count)

    def fulltext(self):

        """Get errors from todays fulltext logs and generate a list for each
        type of error of corresponding bibcodes and source directories. These
        lists are written to files that are further processed in compute.py"""

        # types of errors with corresponding file names
        errors = conf['FULLTEXT_ERRORS']

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
                resp = x.communicate()[0].split("\n")

                for r in resp:
                    if r:
                        r = r.split("'")
                        bibs.append(r[loc_bib])
                        dirs.append(r[loc_dir])

            # create filename based on error message and date
            fname = Filename.get(self.date, FileType.FULLTEXT, adjective=None,
                                 msg="_" + ("_".join(err_msg.split()))
                                 .replace('-', '_') + "_")

            # write bibcodes and directories for each error type to file
            with open(fname, 'w') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerows(list(zip(bibs, dirs)))

            sort(fname)
