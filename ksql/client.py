from __future__ import absolute_import
from __future__ import print_function

from ksql.api import SimplifiedAPI


class KSQLAPI(object):
    """ API Class """

    def __init__(self, url, max_retries=3, check_version=True, **kwargs):
        """
        You can use a Basic Authentication with this API, for now we accept the api_key/secret based on the Confluent
        Cloud implementation. So you just need to put on the kwargs the api_key and secret.
        """
        self.url = url
        self.sa = SimplifiedAPI(url, max_retries=max_retries, **kwargs)
        if check_version is True:
            self.get_ksql_version()

    def get_url(self):
        return self.url

    @property
    def timeout(self):
        return self.sa.get_timout()

    def get_ksql_version(self):
        r = self.sa.get_request(self.url + "/info")
        if r.status_code == 200:
            info = r.json().get("KsqlServerInfo")
            version = info.get("version")
            return version

        else:
            raise ValueError("Status Code: {}.\nMessage: {}".format(r.status_code, r.content))

    def get_properties(self):
        properties = self.sa.ksql("show properties;")
        return properties[0]["properties"]

    def ksql(self, ksql_string, stream_properties=None):
        return self.sa.ksql(ksql_string, stream_properties=stream_properties)

    def query(self, query_string, encoding="utf-8", chunk_size=128, stream_properties=None, idle_timeout=None):
        return self.sa.query(
            query_string=query_string,
            encoding=encoding,
            chunk_size=chunk_size,
            stream_properties=stream_properties,
            idle_timeout=idle_timeout,
        )

    def create_stream(self, table_name, columns_type, topic, value_format="JSON"):
        return self.sa.create_stream(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
        )

    def create_table(self, table_name, columns_type, topic, value_format, key, **kwargs):
        return self.sa.create_table(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format, key=key, **kwargs
        )

    def create_stream_as(
        self,
        table_name,
        select_columns,
        src_table,
        kafka_topic=None,
        value_format="JSON",
        conditions=[],
        partition_by=None,
        **kwargs
    ):

        return self.sa.create_stream_as(
            table_name=table_name,
            select_columns=select_columns,
            src_table=src_table,
            kafka_topic=kafka_topic,
            value_format=value_format,
            conditions=conditions,
            partition_by=partition_by,
            **kwargs,
        )
