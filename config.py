
# where to send solr queries, includes core name
# SOLR_URL = 'http://localhost:19984/solr/collection1/'
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

FT_LOGS = '/proj/ads/kris/logs/'
FT_OUT = '/proj/ads/kris/tmp/'
ERRORS = {"extraction failed for bibcode": FT_LOGS + "adsft.extraction.log*",
          "format not currently supported for extraction": FT_LOGS + "ads-fulltext.log*",
          "is linked to a non-existent file": FT_LOGS + "*.log*",
          "is linked to a zero byte size file": FT_LOGS + "*.log*",
          "No such file or directory": FT_LOGS + "ads-fulltext.log*"
}

proj_home = './'

LOG_LEVEL = 'INFO'
