
from builtins import object
from datetime import datetime, timedelta
from os import remove
from shutil import move
import subprocess

from adsputils import setup_logging, load_config

conf = load_config(proj_home='../')
logger = setup_logging('AutomatedIngestReport',
                       level=conf.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=conf.get('LOG_STDOUT', False))


# enums used to to generate file names
class FileType(object):
    CANONICAL = 'CANONICAL'
    SOLR = 'SOLR'
    FULLTEXT = 'FULLTEXT'


class FileAdjective(object):
    MISSING = 'MISSING'
    DELETED = 'DELETED'
    EXTRA = 'EXTRA'
    NEW = 'NEW'


class Date(object):
    TODAY = 1
    YESTERDAY = 2


class Filename(object):

    @staticmethod
    def get(_date, _type, adjective=None, msg=None):
        """convert passed date, type and adjective into the proper filename"""
        if _date is Date.TODAY:
            _date = datetime.now()
        elif _date is Date.YESTERDAY:
            _date = datetime.now() - timedelta(days=1)
        elif type(_date).__name__ != 'datetime':
            raise ValueError('invalid date passed {}, expected datetime or valid string'.format(_date))

        d = _date.strftime('%Y%m%d')
        if msg and adjective:
            filename = d + msg.lower() + adjective.lower() + _type.capitalize() + '.txt'
        elif msg:
            filename = d + msg.lower() + _type.capitalize() + '.txt'
        elif adjective:
            filename = d + adjective.lower() + _type.capitalize() + '.txt'
        else:
            filename = d + _type.capitalize() + '.txt'
        dir = conf['AIR_DATA_DIRECTORY']
        return dir + filename


def lines_in_file(filename):
    lines = 0
    try:
        for line in open(filename):
            lines += 1
    except Exception as err:
        logger.error('In utils.lines_in_file: %s' % err)
    return lines


def lines_in_file_foo(filename):
    """based on https://gist.github.com/zed/0ac760859e614cd03652#file-gistfile1-py-L41
       works on mac, does not work on unix
    """
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
    for f in [file_in1,file_in2]:
        sorter = 'sort ' + f + ' -o ' + f
        sr = subprocess.call(sorter, shell=True)

    c = 'comm' + ' -2 -3 ' + file_in1 + ' ' + file_in2 + ' > ' + file_out
    r = subprocess.call(c, shell=True)
    if r != 0:
        logger.error('c command returned {}'.format(c, r))
    lines = lines_in_file(file_out)
    return lines


def sort(filename):
    """use temp file and unix sort command essentially sort in place"""

    tmp_filename = '{}.tmp'.format(filename)
    move(filename, tmp_filename)
    c = 'sort {} > {}'.format(tmp_filename, filename)
    r = subprocess.call(c, shell=True)
    if r != 0:
        logger.error('in sort, c command returned {}'.format(c, r))
    remove(tmp_filename)


def occurances_in_file(s, filename):
    """return how many times the string s appears in the passed file"""
    # grep -c doesn't work well ehre
    c = 'grep {} {} | wc -l'.format(s, filename)
    r = subprocess.call(c, shell=True)
    return r
