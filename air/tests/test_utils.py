
import unittest
from datetime import datetime, timedelta
from filecmp import cmp

from air.utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file, conf, lines_in_file
from air.compute import Compute


class TestUtils(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        conf['AIR_DATA_DIRECTORY'] = 'air/tests/stubdata/'

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_filename(self):
        """are file names generated correctly?"""

        dir = conf['AIR_DATA_DIRECTORY']
        _date = datetime.now()
        d = _date.strftime('%Y%m%d')
        filename = Filename.get(Date.TODAY, FileType.SOLR)
        self.assertEqual(dir + d + 'Solr.txt', filename)
        filename = Filename.get(Date.TODAY, FileType.CANONICAL)
        self.assertEqual(dir + d + 'Canonical.txt', filename)

        filename = Filename.get(Date.TODAY, FileType.SOLR, FileAdjective.DELETED)
        self.assertEqual(dir + d + 'deletedSolr.txt', filename)
        filename = Filename.get(Date.TODAY, FileType.CANONICAL, FileAdjective.NEW)
        self.assertEqual(dir + d + 'newCanonical.txt', filename)

        _date = datetime.now() - timedelta(days=1)
        d = _date.strftime('%Y%m%d')
        filename = Filename.get(Date.YESTERDAY, FileType.SOLR)
        self.assertEqual(dir + d + 'Solr.txt', filename)
        filename = Filename.get(Date.YESTERDAY, FileType.CANONICAL, FileAdjective.NEW)
        self.assertEqual(dir + d + 'newCanonical.txt', filename)

        _date = datetime(2000, 1, 1)
        d = _date.strftime('%Y%m%d')
        filename = Filename.get(_date, FileType.SOLR)
        self.assertEqual(dir + d + 'Solr.txt', filename)
        filename = Filename.get(_date, FileType.CANONICAL, FileAdjective.NEW)
        self.assertEqual(dir + d + 'newCanonical.txt', filename)

    def test_wc(self):
        file = Filename.get(datetime(2000, 1, 1), FileType.SOLR)
        length = lines_in_file(file)
        self.assertEqual(2, length)

        file = Filename.get(datetime(2000, 1, 1), FileType.CANONICAL)
        length = lines_in_file(file)
        self.assertEqual(3, length)
