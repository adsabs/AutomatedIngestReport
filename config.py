from __future__ import print_function
import os

LOGGING_LEVEL = 'INFO'
LOG_STDOUT = False

# where to send solr queries, includes core name
SOLR_URL = 'http://localhost:9983/solr/collection1/'

# elasticsearch/kibana url
ELASTICSEARCH_URL = 'https://pipeline-kibana.kube.adslabs.org'

# the current list of canonical bibcodes
CANONICAL_FILE = '/proj/ads_abstracts/config/bibcodes.list.can'

# the count of canonical citations
CANONICAL_CITATION_COUNT = '/proj/ads/abstracts/config/links/citation/COUNT'

SQLALCHEMY_URL_MASTER = 'postgres://master_pipeline:master_pipeline@%s:15432/master_pipeline' % 'adsnest.cfa.harvard.edu'

# home of data files (e.g. a copy of today's canonical bibcodes)
# new files written here, expected files read from here
AIR_DATA_DIRECTORY = '/proj/ads_abstracts/daily_reports/'

# both FULLTEXT_LOGS and KIBANA_TOKEN are placeholders
FULLTEXT_LOGS = '/proj/ads_articles/fulltext/logs/'
KIBANA_TOKEN = 'dummy_token'

if os.path.exists('./local_config.py'):
    from local_config import *
else:
    print('Warning: invalid API token!')

KIBANA_QUERIES = {'"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_myads_pipeline_1\\" +@message:\\"Email sent to\\""': '\nNumber of myADS emails: %s\n',
                  '"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_master_pipeline_1\\" +@message:\\"error sending links\\""': 'Number of Master/Resolver errors: %s\n\n'}

# in double quotes to force exact phrase match during gather

FULLTEXT_ERRORS = {"extraction failed for bibcode":
                   FULLTEXT_LOGS + "adsft.extraction.log*",
                   "format not currently supported for extraction":
                   FULLTEXT_LOGS + "ads-fulltext.log*",
                   "is linked to a non-existent file":
                   FULLTEXT_LOGS + "*.log*",
                   "is linked to a zero byte size file":
                   FULLTEXT_LOGS + "*.log*",
                   "No such file or directory":
                   FULLTEXT_LOGS + "ads-fulltext.log*"
                  }
