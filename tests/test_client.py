import json
import unittest
import requests

import vcr

from ksql import KSqlAPI
import ksql

class TestKSqlAPI(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://ksql-server:8080"
        self.api_client = KSqlAPI(url=self.url)

    def test_with_timeout(self):
        api_client = KSqlAPI(url='http://foo', timeout=10)
        self.assertEquals(api_client.timeout, 10)

    @vcr.use_cassette('tests/vcr_cassettes/get_ksql_server.yml')
    def test_get_ksql_version_success(self):
        """ Test GET requests """
        version = self.api_client.get_ksql_version()
        self.assertEqual(version, '0.2')

    @vcr.use_cassette('tests/vcr_cassettes/ksql_show_table.yml')
    def test_ksql_show_tables(self):
        """ Test GET requests """
        ksql_string = "show tables;"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{'tables': {'statementText': 'show tables;', 'tables': []}}])

    @vcr.use_cassette('tests/vcr_cassettes/ksql_show_table.yml')
    def test_ksql_show_tables_with_no_semicolon(self):
        """ Test GET requests """
        ksql_string = "show tables"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{'tables': {'statementText': 'show tables;', 'tables': []}}])

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_table.yml')
    def test_ksql_create_table(self):
        """ Test GET requests """
        ksql_string = "CREATE STREAM test_table (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='t1', value_format='DELIMITED');"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]['currentStatus']['commandStatus']['status'], 'SUCCESS')

