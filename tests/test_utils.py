import unittest
import vcr
from confluent_kafka import Producer

import ksql.utils as utils
from ksql.client import KSQLAPI


class TestKSQLUtils(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://localhost:8088"
        self.api_client = KSQLAPI(url=self.url, check_version=False)
        self.test_prefix = "ksql_python_test"

        self.exist_topic = self.test_prefix + "_exist_topic"
        self.bootstrap_servers = "localhost:29092"
        if utils.check_kafka_available(self.bootstrap_servers):
            producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            producer.produce(self.exist_topic, "test_message")
            producer.flush()

    def tearDown(self):
        if utils.check_kafka_available(self.bootstrap_servers):
            utils.drop_all_streams(self.api_client, prefix=self.test_prefix)

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_drop_stream.yml")
    def test_drop_stream(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_stream"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        self.assertTrue(utils.get_stream_info(self.api_client, stream_name))
        utils.drop_stream(self.api_client, stream_name)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name))

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_drop_stream_create_as_stream.yml")
    def test_drop_stream_create_as_stream(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_stream"
        stream_name_as = stream_name + "_as"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        ksql_string = "CREATE STREAM {} as select * from {};".format(stream_name_as, stream_name)
        self.api_client.ksql(ksql_string)

        self.assertTrue(utils.get_stream_info(self.api_client, stream_name_as))
        utils.drop_stream(self.api_client, stream_name_as)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name_as))

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_get_all_streams.yml")
    def test_get_all_streams(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_all_streams"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        filtered_streams = utils.get_all_streams(self.api_client, prefix=self.test_prefix)
        self.assertEqual(filtered_streams, [stream_name.upper()])

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_get_stream_info.yml")
    def test_get_stream_info(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_stream_info"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        stream_info = utils.get_stream_info(self.api_client, stream_name)
        # print(stream_info['topic'])
        self.assertEqual(stream_info["topic"], self.exist_topic)

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_drop_all_streams.yml")
    def test_drop_all_streams(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_all_streams"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        utils.drop_all_streams(self.api_client, prefix=self.test_prefix)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name))

    @vcr.use_cassette("tests/vcr_cassettes/utils_test_get_dependent_queries.yml")
    def test_get_dependent_queries(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_dependent_queries"
        stream_name_as = stream_name + "_as"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(
            stream_name, topic
        )
        self.api_client.ksql(ksql_string)
        ksql_string = "CREATE STREAM {} as select * from {};".format(stream_name_as, stream_name)
        self.api_client.ksql(ksql_string)
        read_queries, write_queries = utils.get_dependent_queries(self.api_client, stream_name_as)
        self.assertEqual(read_queries, [])
        self.assertTrue(write_queries[0].startswith("CSAS_KSQL_PYTHON_TEST_TEST_GET_DEPENDENT_QUERIES_AS"))

    def test_parse_columns(self):
        header_str = """[{"header":{"queryId":"none","schema":"`ORDER_ID` INTEGER, `MY_STRUCT` STRUCT<`A` INTEGER, `B` STRING>, `MY_MAP` MAP<STRING, INTEGER>, `MY_ARRAY` ARRAY<INTEGER>, `TOTAL_AMOUNT` DOUBLE, `CUSTOMER_NAME` STRING"}},"""

        columns = utils.parse_columns(header_str)

        self.assertEqual(columns[0], {'name': 'ORDER_ID', 'type': 'INTEGER'})
        self.assertEqual(columns[1], {'name': 'MY_STRUCT', 'type': 'STRUCT'})
        self.assertEqual(columns[2], {'name': 'MY_MAP', 'type': 'MAP'})
        self.assertEqual(columns[3], {'name': 'MY_ARRAY', 'type': 'ARRAY'})
        self.assertEqual(columns[4], {'name': 'TOTAL_AMOUNT', 'type': 'DOUBLE'})
        self.assertEqual(columns[5], {'name': 'CUSTOMER_NAME', 'type': 'STRING'})

    def test_process_row(self):
        parsed_header = [{'name': 'ORDER_ID', 'type': 'INTEGER'}, {'name': 'MY_STRUCT', 'type': 'STRUCT'}, {'name': 'MY_MAP', 'type': 'MAP'}, {'name': 'MY_ARRAY', 'type': 'ARRAY'}, {'name': 'TOTAL_AMOUNT', 'type': 'DOUBLE'}, {'name': 'CUSTOMER_NAME', 'type': 'STRING'}]
        row_str = """{"row":{"columns":[3,{"A":1,"B":"bbb"},{"x":3,"y":4},[1,2,3],43.0,"Palo Alto"]}},\n"""

        row_obj = utils.process_row(row_str, parsed_header)

        self.assertEqual(row_obj["ORDER_ID"], 3)
        self.assertEqual(row_obj["MY_STRUCT"], {"A": 1, "B": "bbb"})
        self.assertEqual(row_obj["MY_MAP"], {"x": 3, "y": 4})
        self.assertEqual(row_obj["MY_ARRAY"], [1, 2, 3])
        self.assertEqual(row_obj["TOTAL_AMOUNT"], 43)
        self.assertEqual(row_obj["CUSTOMER_NAME"], "Palo Alto")

    def test_process_query_result(self):
        def mock_generator():
            results = [1,2,3,4,5,6]
            for a in results:
                yield a

        results = utils.process_query_result(mock_generator())

        first_result = next(results)
        self.assertEqual(first_result, 1)

    def test_process_query_result_parse_rows(self):
        def mock_generator():
            header_str = """[{"header":{"queryId":"none","schema":"`ORDER_ID` INTEGER, `MY_STRUCT` STRUCT<`A` INTEGER, `B` STRING>, `MY_MAP` MAP<STRING, INTEGER>, `MY_ARRAY` ARRAY<INTEGER>, `TOTAL_AMOUNT` DOUBLE, `CUSTOMER_NAME` STRING"}},"""
            row_str = """{"row":{"columns":[3,{"A":1,"B":"bbb"},{"x":3,"y":4},[1,2,3],43.0,"Palo Alto"]}},\n"""

            results = [header_str, row_str]
            for a in results:
                yield a

        rows = utils.process_query_result(mock_generator(), True)

        first_row = next(rows)
        self.assertEqual(first_row["ORDER_ID"], 3)
        self.assertEqual(first_row["MY_STRUCT"], {"A": 1, "B": "bbb"})
        self.assertEqual(first_row["MY_MAP"], {"x": 3, "y": 4})
        self.assertEqual(first_row["MY_ARRAY"], [1, 2, 3])
        self.assertEqual(first_row["TOTAL_AMOUNT"], 43)
        self.assertEqual(first_row["CUSTOMER_NAME"], "Palo Alto")