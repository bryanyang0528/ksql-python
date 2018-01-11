import json
import unittest
import requests

import vcr

import ksql
from ksql import KSQLAPI
from ksql import SQLBuilder
from ksql.error import CreateError

class TestKSQLAPI(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://ksql-server:8080"
        self.api_client = KSQLAPI(url=self.url)

    def test_with_timeout(self):
        api_client = KSQLAPI(url='http://foo', timeout=10)
        self.assertEquals(api_client.timeout, 10)

    @vcr.use_cassette('tests/vcr_cassettes/healthcheck.yml')
    def test_ksql_server_healthcheck(self):
        """ Test GET requests """
        res = requests.get(self.url)
        self.assertEqual(res.status_code, 200)

    @vcr.use_cassette('tests/vcr_cassettes/get_ksql_server.yml')
    def test_get_ksql_version_success(self):
        """ Test GET requests """
        version = self.api_client.get_ksql_version()
        self.assertEqual(version, ksql.__ksql_server_version__)

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

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream.yml')
    def test_ksql_create_stream(self):
        """ Test GET requests """
        ksql_string = "CREATE STREAM test_table (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='t1', value_format='DELIMITED');"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]['currentStatus']['commandStatus']['status'], 'SUCCESS')

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream.yml')
    def test_ksql_create_stream_by_builder(self):
        sql_type = 'create'
        table_type = 'stream'
        table_name = 'test_table'
        columns_type = ['viewtime bigint',
                        'userid varchar',
                        'pageid varchar']
        topic = 't1'
        value_format = 'DELIMITED'
        
        ksql_string = SQLBuilder.build(sql_type = sql_type, 
                                      table_type = table_type, 
                                      table_name = table_name, 
                                      columns_type = columns_type, 
                                      topic = topic, 
                                      value_format = value_format)

        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]['currentStatus']['commandStatus']['status'], 'SUCCESS')

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream.yml')
    def test_ksql_create_stream_by_builder_api(self):
        table_name = 'test_table'
        columns_type = ['viewtime bigint',
                        'userid varchar',
                        'pageid varchar']
        topic = 't1'
        value_format = 'DELIMITED'
        
        r = self.api_client.create_stream(table_name = table_name, 
                                      columns_type = columns_type, 
                                      topic = topic, 
                                      value_format = value_format)

        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_topic_already_registered.yml')
    def test_table_already_registered_error(self):
        table_name = 'foo_table' 
        columns_type = ['name string', 'age bigint'] 
        topic = 't1' 
        value_format = 'DELIMITED'

        with self.assertRaises(CreateError):
            r = self.api_client.create_stream(table_name = table_name, 
                                              columns_type = columns_type, 
                                              topic = topic, 
                                              value_format = value_format)



