import unittest
from ksql.client import KSQLAPI
import ksql.utils as utils
from confluent_kafka import Producer, Consumer
import vcr

class TestKSQLUtils(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://localhost:8088"
        self.api_client = KSQLAPI(url=self.url)
        self.test_prefix = "ksql_python_test"

        self.exist_topic = self.test_prefix + '_exist_topic'
        self.bootstrap_servers = 'localhost:29092'
        if utils.check_kafka_available(self.bootstrap_servers):
            producer = Producer({'bootstrap.servers': self.bootstrap_servers})
            producer.produce(self.exist_topic, "test_message")
            producer.flush()

    def tearDown(self):
        if utils.check_kafka_available(self.bootstrap_servers):
            utils.drop_all_streams(self.api_client, prefix=self.test_prefix)

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_drop_stream.yml')
    def test_drop_stream(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_stream"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        self.assertTrue(utils.get_stream_info(self.api_client, stream_name))
        utils.drop_stream(self.api_client, stream_name)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name))

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_drop_stream_create_as_stream.yml')
    def test_drop_stream_create_as_stream(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_stream"
        stream_name_as = stream_name + "_as"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        ksql_string = "CREATE STREAM {} as select * from {};".format(stream_name_as, stream_name)
        r = self.api_client.ksql(ksql_string)

        self.assertTrue(utils.get_stream_info(self.api_client, stream_name_as))
        utils.drop_stream(self.api_client, stream_name_as)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name_as))

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_get_all_streams.yml')
    def test_get_all_streams(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_all_streams"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        filtered_streams = utils.get_all_streams(self.api_client, prefix=self.test_prefix)
        self.assertEqual(filtered_streams, [stream_name.upper()])

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_get_stream_info.yml')
    def test_get_stream_info(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_stream_info"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        stream_info = utils.get_stream_info(self.api_client, stream_name)
        #print(stream_info['topic'])
        self.assertEqual(stream_info['topic'], self.exist_topic)

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_drop_all_streams.yml')
    def test_drop_all_streams(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_drop_all_streams"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                        WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        utils.drop_all_streams(self.api_client, prefix=self.test_prefix)
        self.assertFalse(utils.get_stream_info(self.api_client, stream_name))

    @vcr.use_cassette('tests/vcr_cassettes/utils_test_get_dependent_queries.yml')
    def test_get_dependent_queries(self):
        topic = self.exist_topic
        stream_name = self.test_prefix + "_test_get_dependent_queries"
        stream_name_as = stream_name + "_as"
        utils.drop_stream(self.api_client, stream_name)
        ksql_string = "CREATE STREAM {} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{}', value_format='DELIMITED');".format(stream_name, topic)
        r = self.api_client.ksql(ksql_string)
        ksql_string = "CREATE STREAM {} as select * from {};".format(stream_name_as, stream_name)
        r = self.api_client.ksql(ksql_string)
        read_queries, write_queries = utils.get_dependent_queries(self.api_client, stream_name_as)
        self.assertEqual(read_queries, [])
        self.assertTrue(write_queries[0].startswith('CSAS_KSQL_PYTHON_TEST_TEST_GET_DEPENDENT_QUERIES_AS'))