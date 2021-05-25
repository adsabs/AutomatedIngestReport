from __future__ import print_function
import os

LOGGING_LEVEL = 'INFO'
LOG_STDOUT = False

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
