
from datetime import datetime, timedelta
import subprocess

from adsputils import setup_logging, load_config

logger = setup_logging('AutomatedIngestReport')
conf = load_config(proj_home='./')


# enums used to to generate file names
class FileType:
    CANONICAL = 'CANONICAL'
    SOLR = 'SOLR'


class FileAdjective:
    MISSING = 'MISSING'
    DELETED = 'DELETED'
    EXTRA = 'EXTRA'
    NEW = 'NEW'


class Date:
    TODAY = 1
    YESTERDAY = 2


class Filename:

    @staticmethod
    def get(_date, _type, adjective=None):
        """convert passed date, type and adjective into the proper filename"""
        if _date is Date.TODAY:
            _date = datetime.now()
        elif _date is Date.YESTERDAY:
            _date = datetime.now() - timedelta(days=1)
        elif type(_date).__name__ != 'datetime':
            raise ValueError('invalid date passed {}, expected datetime or valid string'.format(_date))

        d = _date.strftime('%Y%m%d')
        if adjective:
            filename = d + adjective.lower() + _type.capitalize() + '.txt'
        else:
            filename = d + _type.capitalize() + '.txt'
        dir = conf['AIR_DATA_DIRECTORY']
        return dir + filename


def lines_in_file(filename):
    """based on https://gist.github.com/zed/0ac760859e614cd03652#file-gistfile1-py-L41"""
    import os.path
    if os.path.isfile(filename) is False:
        raise ValueError('passed file does not exist')

    out = subprocess.Popen(['wc', '-l', filename],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]
    # out has a value like
    # ('', ' ', '      1 /Users/SpacemanSteve/code/eclipse/workspace/air/air/tests/stubdata/20000102missingSolr.txt\n')
    count = int(out.partition(' ')[2].lstrip().split(' ')[0])
    return count


def comm(file_in1, file_in2, file_out):
    """run unix comm command to generated what's in the first file but not the second"""

    c = 'comm' + ' -2 -3 ' + file_in1 + ' ' + file_in2 + ' > ' + file_out
    r = subprocess.call(c, shell=True)
    if r != 0:
        logger.error('c command returned {}'.format(c, r))
    lines = lines_in_file(file_out)
    return lines
