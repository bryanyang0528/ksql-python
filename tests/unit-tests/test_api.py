import unittest
import responses
import urllib

from ksql.api import BaseAPI


class TestBaseApi(unittest.TestCase):
    @responses.activate
    def test_base_api_query(self):
        responses.add(responses.POST, "http://dummy.org/query", body="test", status=200, stream=True)
        base = BaseAPI("http://dummy.org")
        result = base.query("so")
        with self.assertRaises(urllib.error.HTTPError):
            for entry in result:
                entry
