import os

# where to send solr queries, includes core name
SOLR_URL = 'http://localhost:9983/solr/collection1/'

# the current list of canonical bibcodes
CANONICAL_FILE = '/proj/ads/abstracts/config/bibcodes.list.can'

ELASTICSEARCH_URL = 'https://search-pipeline-d6gsitdlgp2dh25slmlrcwjtse.us-east-1.es.amazonaws.com'

SQLALCHEMY_URL_MASTER = 'postgres://master_pipeline:master_pipeline@%s:15432/master_pipeline' % 'adsqb.cfa.harvard.edu'
SQLALCHEMY_URL_NONBIB = 'postgres://data_pipeline:data_pipeline@%s:15432/data_pipeline' % 'adsqb.cfa.harvard.edu'

#AIR_DIRECTORY = '/Users/SpacemanSteve/code/eclipse/workspace/air/air/tests/stubdata/'
# home of data files (e.g. a copy of today's canonical bibcodes)
# new files written here, expected files read from here
AIR_DATA_DIRECTORY = './data/'

FULLTEXT_LOGS = '/proj/ads/articles/fulltext/logs/'

## in double quotes to force exact phrase match during gather 
#FULLTEXT_ERRORS = ['"extraction failed for bibcode"',
#                   '"format not currently supported for extraction"',
#                   '"is linked to a non-existent file"',
#                   '"is linked to a zero byte size file"',
#                   '"[Errno 2] No such file or directory"']

FULLTEXT_ERRORS = {"extraction failed for bibcode": FULLTEXT_LOGS + "adsft.extraction.log*",
          "format not currently supported for extraction": FULLTEXT_LOGS + "ads-fulltext.log*",
          "is linked to a non-existent file": FULLTEXT_LOGS + "*.log*",
          "is linked to a zero byte size file": FULLTEXT_LOGS + "*.log*",
          "No such file or directory": FULLTEXT_LOGS + "ads-fulltext.log*"
}

LOG_LEVEL = 'INFO'

