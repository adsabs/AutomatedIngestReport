
import unittest
from datetime import datetime, timedelta
from filecmp import cmp

from air.utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file, conf, lines_in_file
from air.compute import Compute


class TestCompute(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        conf['AIR_DATA_DIRECTORY'] = 'air/tests/stubdata/'

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_compute(self):
        """ run compute and compare generated files to known good files

        stub_data directory includes script to create good files for comparision"""

        start = datetime(2000, 1, 1)
        end = datetime(2000, 1, 2)
        c = Compute(start, end)
        c.canonical()
        c.solr()
        c.fulltext()

        filename = Filename.get(end, FileType.CANONICAL, FileAdjective.NEW)
        filegood = filename.replace('.txt', '.good')
        self.assertTrue(cmp(filegood, filename), 'canonical new')

        filename = Filename.get(end, FileType.CANONICAL, FileAdjective.DELETED)
        filegood = filename.replace('.txt', '.good')
        self.assertTrue(cmp(filegood, filename), 'canonical deleted')

        filename = Filename.get(end, FileType.SOLR, FileAdjective.MISSING)
        filegood = filename.replace('.txt', '.good')
        self.assertTrue(cmp(filegood, filename), 'solr missing')

        filename = Filename.get(end, FileType.SOLR, FileAdjective.NEW)
        filegood = filename.replace('.txt', '.good')
        self.assertTrue(cmp(filegood, filename), 'solr new')

        filename = Filename.get(end, FileType.SOLR, FileAdjective.DELETED)
        filegood = filename.replace('.txt', '.good')
        self.assertTrue(cmp(filegood, filename), 'solr deleted')

        for err in conf['FULLTEXT_ERRORS']:

            self.assertTrue(cmp('new.good', 'new.txt'), err + ' new')
            self.assertTrue(cmp('fixed.good', 'fixed.txt'), err + ' fixed')
