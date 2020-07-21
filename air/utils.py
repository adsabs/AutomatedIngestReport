
from datetime import datetime, timedelta
from os import remove
from shutil import move
import subprocess
import os

from adsputils import setup_logging, load_config

logger = setup_logging('AutomatedIngestReport')
conf = load_config(proj_home='./')


# enums used to to generate file names
class FileType:
    CANONICAL = 'CANONICAL'
    SOLR = 'SOLR'
    FULLTEXT = 'FULLTEXT'


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
    except Exception, err:
        logger.error("error in utils.lines_in_file: %s" % err)
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

def sorter(path):
    """ to arrange files by date in the filename to be able to obtain the two most
        recent files from the directory"""

    filename = os.path.basename(path)
    return datetime.strptime(filename[0:10], '%Y-%m-%d')

def remove_duplicates(filename):
    """use temp file and unix uniq command remove duplicates in place"""

    tmp_filename = '{}.tmp'.format(filename)
    move(filename, tmp_filename)
    c = 'uniq {} > {}'.format(tmp_filename, filename)
    r = subprocess.call(c, shell=True)
    if r != 0:
	logger.error('in sort, c command returned {}'.format(c, r))
    remove(tmp_filename)

