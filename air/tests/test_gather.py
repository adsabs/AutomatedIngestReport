
import unittest
from mock import patch, Mock
from requests import Response

from air.gather import Gather
from air.utils import Filename, Date, FileType, conf


class TestUtils(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        conf['AIR_DATA_DIRECTORY'] = 'air/tests/tmp/'

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def test_gather_solr(self):
        """mock solr response with two bibcodes"""

        # gather.solr uses batch api lots of mocks needed
        initial = Mock()
        initial.status_code = 200
        initial.json.return_value = {'jobid': 'foobar'}
        start = Mock()
        start.status_code = 200
        not_finished = Mock()
        not_finished.status_code = 200
        not_finished.json.return_value = {'job-status': 'not finished'}
        finished = Mock()
        finished.status_code = 200
        finished.json.return_value = {'job-status': 'finished'}
        data = Mock()
        data.status_code = 200
        data.text = '{"bibcode":"2003ASPC..295..361M"},\n{"bibcode":"2003ASPC..295..361Z"}'
        with patch('requests.get') as r:
            r.side_effect = [initial, start, not_finished, finished, data]
            g = Gather()
            success = g.solr_bibcodes()
        # self.assertTrue(success)
        filename = Filename.get(Date.TODAY, FileType.SOLR)
        f = open(filename, 'r')
        line = f.readline()
        self.assertEqual('2003ASPC..295..361M', line.strip())
        line = f.readline()
        self.assertEqual('2003ASPC..295..361Z', line.strip())
        f.close()
