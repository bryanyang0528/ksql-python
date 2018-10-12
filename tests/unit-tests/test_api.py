import unittest
import responses

from ksql.api import BaseAPI


class TestBaseApi(unittest.TestCase):

    @responses.activate
    def test_base_api_query(self):
        responses.add(responses.POST, 'http://dummy.org/query',
                      body="test", status=200,
                      stream=True)
        base = BaseAPI("http://dummy.org")
        result = base.query("so")
        for entry in result:
            self.assertEqual(entry, "test")
