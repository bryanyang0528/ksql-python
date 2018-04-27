import json
import unittest
import requests
from copy import copy

import vcr

import ksql
from ksql import KSQLAPI
from ksql import SQLBuilder
from ksql.errors import CreateError


class TestKSQLAPI(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://ksql-server:8080"
        self.api_client = KSQLAPI(url=self.url)
        self.exist_topic = 'exist_topic'

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
        topic = self.exist_topic
        ksql_string = "CREATE STREAM test_table (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(topic)
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
        topic = self.exist_topic
        value_format = 'DELIMITED'

        ksql_string = SQLBuilder.build(sql_type=sql_type,
                                       table_type=table_type,
                                       table_name=table_name,
                                       columns_type=columns_type,
                                       topic=topic,
                                       value_format=value_format)

        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]['currentStatus']['commandStatus']['status'], 'SUCCESS')

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream.yml')
    def test_ksql_create_stream_by_builder_api(self):
        table_name = 'test_table'
        columns_type = ['viewtime bigint',
                        'userid varchar',
                        'pageid varchar']
        topic = self.exist_topic
        value_format = 'DELIMITED'

        r = self.api_client.create_stream(table_name=table_name,
                                          columns_type=columns_type,
                                          topic=topic,
                                          value_format=value_format)

        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_topic_already_registered.yml')
    def test_raise_create_error_topic_already_registered(self):
        table_name = 'foo_table'
        columns_type = ['name string', 'age bigint']
        topic = self.exist_topic
        value_format = 'DELIMITED'

        r = self.api_client.create_stream(table_name=table_name,
                                          columns_type=columns_type,
                                          topic=topic,
                                          value_format=value_format)

        with self.assertRaises(CreateError):
            r = self.api_client.create_stream(table_name=table_name,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)

    @vcr.use_cassette('tests/vcr_cassettes/raise_create_error_no_topic.yml')
    def test_raise_create_error_no_topic(self):
        table_name = 'foo_table'
        columns_type = ['name string', 'age bigint']
        topic = 'this_topic_is_not_exist'
        value_format = 'DELIMITED'

        with self.assertRaises(CreateError):
            r = self.api_client.create_stream(table_name=table_name,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream_as_without_conditions.yml')
    def test_create_stream_as_without_conditions(self):

        src_table = 'pageviews_original'
        columns_type = ['name string', 'age bigint', 'userid string', 'pageid bigint']
        topic = self.exist_topic

        table_name = 'create_stream_as_without_conditions'
        kafka_topic = 'create_stream_as_without_conditions'
        value_format = 'DELIMITED'
        select_columns = ['rowtime as logtime', '*']

        try:
            r = self.api_client.create_stream(table_name=src_table,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)
        except CreateError as e:
            pass

        r = self.api_client.create_stream_as(table_name=table_name,
                                             src_table=src_table,
                                             kafka_topic=kafka_topic,
                                             select_columns=select_columns,
                                             timestamp='logtime',
                                             value_format=value_format)
        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream_as_with_conditions_without_startwith.yml')
    def test_create_stream_as_with_conditions_without_startwith(self):

        src_table = 'pageviews_original'
        columns_type = ['name string', 'age bigint', 'userid string', 'pageid bigint']
        topic = self.exist_topic

        table_name = 'create_stream_as_with_conditions_without_startwith'
        kafka_topic = 'create_stream_as_with_conditions_without_startwith'
        value_format = 'DELIMITED'
        select_columns = ['rowtime as logtime', '*']
        conditions = "userid = 'foo'"

        try:
            r = self.api_client.create_stream(table_name=src_table,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)
        except CreateError as e:
            pass

        r = self.api_client.create_stream_as(table_name=table_name,
                                             src_table=src_table,
                                             kafka_topic=kafka_topic,
                                             select_columns=select_columns,
                                             timestamp='logtime',
                                             value_format=value_format,
                                             conditions=conditions)

        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream_as_with_conditions_with_startwith.yml')
    def test_create_stream_as_with_conditions_with_startwith(self):

        src_table = 'pageviews_original'
        columns_type = ['name string', 'age bigint', 'userid string', 'pageid bigint']
        topic = self.exist_topic

        table_name = 'create_stream_as_with_conditions_with_startwith'
        kafka_topic = 'create_stream_as_with_conditions_with_startwith'
        value_format = 'DELIMITED'
        select_columns = ['rowtime as logtime', '*']
        conditions = "userid = 'foo_%'"

        try:
            r = self.api_client.create_stream(table_name=src_table,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)
        except CreateError as e:
            pass

        r = self.api_client.create_stream_as(table_name=table_name,
                                             src_table=src_table,
                                             kafka_topic=kafka_topic,
                                             select_columns=select_columns,
                                             timestamp='logtime',
                                             value_format=value_format,
                                             conditions=conditions)

        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream_as_with_conditions_with_startwith_with_and.yml')
    def test_create_stream_as_with_conditions_with_startwith_with_and(self):

        src_table = 'pageviews_original'
        columns_type = ['name string', 'age bigint', 'userid string', 'pageid bigint']
        topic = self.exist_topic

        table_name = 'create_stream_as_with_conditions_with_startwith_with_and'
        kafka_topic = 'create_stream_as_with_conditions_with_startwith_with_and'
        value_format = 'DELIMITED'
        select_columns = ['rowtime as logtime', '*']
        conditions = "userid = 'foo_%' and age > 10"

        try:
            r = self.api_client.create_stream(table_name=src_table,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)
        except CreateError as e:
            pass

        r = self.api_client.create_stream_as(table_name=table_name,
                                             src_table=src_table,
                                             kafka_topic=kafka_topic,
                                             select_columns=select_columns,
                                             timestamp='logtime',
                                             value_format=value_format,
                                             conditions=conditions)

        self.assertTrue(r)

    @vcr.use_cassette('tests/vcr_cassettes/ksql_create_stream_as_with_wrong_timestamp.yml')
    def test_ksql_create_stream_as_with_wrong_timestamp(self):
        src_table = 'prebid_traffic_log_total_stream'
        columns_type = ['name string', 'age bigint', 'userid string', 'pageid bigint']
        topic = self.exist_topic

        table_name = 'prebid_traffic_log_valid_stream'
        kafka_topic = 'prebid_traffic_log_valid_topic'
        value_format = 'DELIMITED'
        select_columns = ['*']
        timestamp = 'foo'

        try:
            r = self.api_client.create_stream(table_name=src_table,
                                              columns_type=columns_type,
                                              topic=topic,
                                              value_format=value_format)
        except CreateError as e:
            pass

        with self.assertRaises(CreateError):
            r = self.api_client.create_stream_as(table_name=table_name,
                                                 src_table=src_table,
                                                 kafka_topic=kafka_topic,
                                                 select_columns=select_columns,
                                                 timestamp=timestamp,
                                                 value_format=value_format)
