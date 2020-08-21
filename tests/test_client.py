import requests
import unittest
import vcr
from confluent_kafka import Producer

import ksql
import ksql.utils as utils
from ksql import KSQLAPI
from ksql import SQLBuilder
from ksql.errors import KSQLError


class TestKSQLAPI(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://localhost:8088"
        self.api_client = KSQLAPI(url=self.url, check_version=False)
        self.test_prefix = "ksql_python_test"
        self.exist_topic = "exist_topic"
        self.bootstrap_servers = "localhost:29092"
        if utils.check_kafka_available(self.bootstrap_servers):
            producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            producer.produce(self.exist_topic, "test_message")
            producer.flush()

    def tearDown(self):
        if utils.check_kafka_available(self.bootstrap_servers):
            utils.drop_all_streams(self.api_client)

    def test_get_url(self):
        self.assertEqual(self.api_client.get_url(), "http://localhost:8088")

    def test_with_timeout(self):
        api_client = KSQLAPI(url=self.url, timeout=10, check_version=False)
        self.assertEqual(api_client.timeout, 10)

    @vcr.use_cassette("tests/vcr_cassettes/healthcheck.yml")
    def test_ksql_server_healthcheck(self):
        """ Test GET requests """
        res = requests.get(self.url + "/status")
        self.assertEqual(res.status_code, 200)

    @vcr.use_cassette("tests/vcr_cassettes/get_ksql_server.yml")
    def test_get_ksql_version_success(self):
        """ Test GET requests """
        version = self.api_client.get_ksql_version()
        self.assertEqual(version, ksql.__ksql_server_version__)

    @vcr.use_cassette("tests/vcr_cassettes/get_properties.yml")
    def test_get_properties(self):
        properties = self.api_client.get_properties()
        property = [i for i in properties if i["name"] == "ksql.schema.registry.url"][0]
        self.assertEqual(property.get("value"), "http://schema-registry:8081")

    @vcr.use_cassette("tests/vcr_cassettes/ksql_show_table_with_api_key.yml")
    def test_ksql_show_tables_with_api_key(self):
        api_client = KSQLAPI(url=self.url, check_version=False, api_key='foo', secret='bar')
        ksql_string = "show tables;"
        r = api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    @vcr.use_cassette("tests/vcr_cassettes/ksql_show_table.yml")
    def test_ksql_show_tables(self):
        """ Test GET requests """
        ksql_string = "show tables;"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    @vcr.use_cassette("tests/vcr_cassettes/ksql_show_table.yml")
    def test_ksql_show_tables_with_no_semicolon(self):
        """ Test GET requests """
        ksql_string = "show tables"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream.yml")
    def test_ksql_create_stream(self):
        """ Test GET requests """
        topic = self.exist_topic
        stream_name = self.test_prefix + "test_ksql_create_stream"
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

    @unittest.skipIf(not utils.check_kafka_available("localhost:29092"), "vcrpy does not support streams yet")
    def test_ksql_create_stream_w_properties(self):
        """ Test GET requests """
        topic = self.exist_topic
        stream_name = "TEST_KSQL_CREATE_STREAM"
        ksql_string = "CREATE STREAM {} (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
                       WITH (kafka_topic='{}', value_format='JSON');".format(
            stream_name, topic
        )
        streamProperties = {"ksql.streams.auto.offset.reset": "earliest"}

        if "TEST_KSQL_CREATE_STREAM" not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=streamProperties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(self.exist_topic, """{"order_id":3,"total_amount":43,"customer_name":"Palo Alto"}""")
        producer.flush()
        chunks = self.api_client.query(
            "select * from {} EMIT CHANGES".format(stream_name), stream_properties=streamProperties
        )

        for chunk in chunks:
            self.assertTrue(chunk)
            break

    @unittest.skipIf(not utils.check_kafka_available("localhost:29092"), "vcrpy does not support streams yet")
    def test_ksql_parse_query_result_with_utils(self):
        topic = self.exist_topic
        stream_name = "TEST_KSQL_PARSE_QUERY_WITH_UTILS"
        ksql_string = "CREATE STREAM {} (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
                       WITH (kafka_topic='{}', value_format='JSON');".format(
            stream_name, topic
        )
        streamProperties = {"ksql.streams.auto.offset.reset": "earliest"}

        if stream_name not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=streamProperties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(self.exist_topic, """{"order_id":3,"total_amount":43,"customer_name":"Palo Alto"}""")
        producer.flush()
        chunks = self.api_client.query(
            "select * from {} EMIT CHANGES".format(stream_name), stream_properties=streamProperties
        )
        header = next(chunks)
        columns = utils.parse_columns(header)

        for chunk in chunks:
            row_obj = utils.process_row(chunk, columns)
            self.assertEqual(row_obj["ORDER_ID"], 3)
            self.assertEqual(row_obj["TOTAL_AMOUNT"], 43)
            self.assertEqual(row_obj["CUSTOMER_NAME"], "Palo Alto")
            break

    @unittest.skipIf(not utils.check_kafka_available("localhost:29092"), "vcrpy does not support streams yet")
    def test_ksql_parse_query_result(self):
        topic = self.exist_topic
        stream_name = "TEST_KSQL_PARSE_QUERY"
        ksql_string = "CREATE STREAM {} (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
                       WITH (kafka_topic='{}', value_format='JSON');".format(
            stream_name, topic
        )
        streamProperties = {"ksql.streams.auto.offset.reset": "earliest"}

        if stream_name not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=streamProperties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(self.exist_topic, """{"order_id":3,"total_amount":43,"customer_name":"Palo Alto"}""")
        producer.flush()
        chunks = self.api_client.query(
            "select * from {} EMIT CHANGES".format(stream_name), stream_properties=streamProperties, return_objects=True
        )

        for chunk in chunks:
            self.assertEqual(chunk["ORDER_ID"], 3)
            self.assertEqual(chunk["TOTAL_AMOUNT"], 43)
            self.assertEqual(chunk["CUSTOMER_NAME"], "Palo Alto")
            break

    @vcr.use_cassette("tests/vcr_cassettes/bad_requests.yml")
    def test_bad_requests(self):
        broken_ksql_string = "noi"
        with self.assertRaises(KSQLError) as e:
            self.api_client.ksql(broken_ksql_string)

        exception = e.exception
        self.assertEqual(exception.error_code, 40001)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_by_builder.yml")
    def test_ksql_create_stream_by_builder(self):
        sql_type = "create"
        table_type = "stream"
        table_name = "test_table"
        columns_type = ["viewtime bigint", "userid varchar", "pageid varchar"]
        topic = self.exist_topic
        value_format = "DELIMITED"

        utils.drop_stream(self.api_client, table_name)

        ksql_string = SQLBuilder.build(
            sql_type=sql_type,
            table_type=table_type,
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
        )

        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_by_builder_api.yml")
    def test_ksql_create_stream_by_builder_api(self):
        table_name = "test_table"
        columns_type = ["viewtime bigint", "userid varchar", "pageid varchar"]
        topic = self.exist_topic
        value_format = "DELIMITED"

        utils.drop_stream(self.api_client, table_name)

        r = self.api_client.create_stream(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
        )

        self.assertTrue(r)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_topic_already_registered.yml")
    def test_raise_create_error_topic_already_registered(self):
        table_name = "foo_table"
        columns_type = ["name string", "age bigint"]
        topic = self.exist_topic
        value_format = "DELIMITED"
        utils.drop_stream(self.api_client, table_name)
        self.api_client.create_stream(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
        )

        with self.assertRaises(KSQLError):
            self.api_client.create_stream(
                table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
            )

    @vcr.use_cassette("tests/vcr_cassettes/raise_create_error_no_topic.yml")
    def test_raise_create_error_no_topic(self):
        table_name = "foo_table"
        columns_type = ["name string", "age bigint"]
        topic = "this_topic_is_not_exist"
        value_format = "DELIMITED"

        with self.assertRaises(KSQLError):
            self.api_client.create_stream(
                table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
            )

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_as_without_conditions.yml")
    def test_create_stream_as_without_conditions(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.exist_topic

        table_name = "create_stream_as_without_conditions"
        kafka_topic = "create_stream_as_without_conditions"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
        )
        self.assertTrue(r)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_as_with_conditions_without_startwith.yml")
    def test_create_stream_as_with_conditions_without_startwith(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.exist_topic

        table_name = "create_stream_as_with_conditions_without_startwith"
        kafka_topic = "create_stream_as_with_conditions_without_startwith"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo'"

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_as_with_conditions_with_startwith.yml")
    def test_create_stream_as_with_conditions_with_startwith(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.exist_topic

        table_name = "create_stream_as_with_conditions_with_startwith"
        kafka_topic = "create_stream_as_with_conditions_with_startwith"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo_%'"
        utils.drop_stream(self.api_client, src_table)
        utils.drop_stream(self.api_client, table_name)

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_as_with_conditions_with_startwith_with_and.yml")
    def test_create_stream_as_with_conditions_with_startwith_with_and(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.exist_topic

        table_name = "create_stream_as_with_conditions_with_startwith_with_and"
        kafka_topic = "create_stream_as_with_conditions_with_startwith_with_and"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo_%' and age > 10"

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    @vcr.use_cassette("tests/vcr_cassettes/ksql_create_stream_as_with_wrong_timestamp.yml")
    def test_ksql_create_stream_as_with_wrong_timestamp(self):
        src_table = "prebid_traffic_log_total_stream"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.exist_topic

        table_name = "prebid_traffic_log_valid_stream"
        kafka_topic = "prebid_traffic_log_valid_topic"
        value_format = "DELIMITED"
        select_columns = ["*"]
        timestamp = "foo"
        utils.drop_stream(self.api_client, src_table)
        utils.drop_stream(self.api_client, table_name)
        try:
            self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        with self.assertRaises(KSQLError):
            self.api_client.create_stream_as(
                table_name=table_name,
                src_table=src_table,
                kafka_topic=kafka_topic,
                select_columns=select_columns,
                timestamp=timestamp,
                value_format=value_format,
            )
