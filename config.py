from __future__ import print_function
import os

# where to send solr queries, includes core name
SOLR_URL = 'http://localhost:9983/solr/collection1/'

# the current list of canonical bibcodes
CANONICAL_FILE = '/proj/ads/abstracts/config/bibcodes.list.can'

ELASTICSEARCH_URL = 'https://search-pipeline-d6gsitdlgp2dh25slmlrcwjtse.us-east-1.es.amazonaws.com'

SQLALCHEMY_URL_MASTER = 'postgres://master_pipeline:master_pipeline@%s:15432/master_pipeline' % 'adsqb.cfa.harvard.edu'
SQLALCHEMY_URL_NONBIB = 'postgres://data_pipeline:data_pipeline@%s:15432/data_pipeline' % 'adsqb.cfa.harvard.edu'

# home of data files (e.g. a copy of today's canonical bibcodes)
# new files written here, expected files read from here
AIR_DATA_DIRECTORY = '/proj/ads_abstracts/daily_reports/'

FULLTEXT_LOGS = '/proj/ads/articles/fulltext/logs/'

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

LOG_LEVEL = 'INFO'

KIBANA_TOKEN = 'dummy_token'
if os.path.exists('./local_config.py'):
    from local_config import *
else:
    print('Warning: invalid API token!')
