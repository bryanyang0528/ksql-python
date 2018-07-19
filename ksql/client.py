from __future__ import absolute_import
from __future__ import print_function

import requests

from ksql.api import SimplifiedAPI


class KSQLAPI(object):
    """ API Class """

    def __init__(self, url, max_retries=3, check_version=True, **kwargs):
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
        r = requests.get(self.url + "/info")
        if r.status_code == 200:
            info = r.json().get('KsqlServerInfo')
            version = info.get('version')
            return version

        else:
            raise ValueError(
                'Status Code: {}.\nMessage: {}'.format(
                    r.status_code, r.content))

    def get_properties(self):
        properties = self.sa.ksql("show properties;")
        return properties[0]['properties']['properties']

    def ksql(self, ksql_string):
        return self.sa.ksql(ksql_string)

    def query(self, query_string, encoding='utf-8', chunk_size=128):
        return self.sa.query(query_string=query_string,
                      encoding=encoding,
                      chunk_size=chunk_size)

    def create_stream(
            self,
            table_name,
            columns_type,
            topic,
            value_format='JSON'):
        return self.sa.create_stream(table_name=table_name,
                                     columns_type=columns_type,
                                     topic=topic,
                                     value_format=value_format)

    def create_table(self, table_name, columns_type, topic, value_format):
        return self.sa.create_table(table_name=table_name,
                                    columns_type=columns_type,
                                    topic=topic,
                                    value_format=value_format)

    def create_stream_as(
            self,
            table_name,
            select_columns,
            src_table,
            kafka_topic=None,
            value_format='JSON',
            conditions=[],
            partition_by=None,
            **kwargs):

        return self.sa.create_stream_as(table_name=table_name,
                                        select_columns=select_columns,
                                        src_table=src_table,
                                        kafka_topic=kafka_topic,
                                        value_format=value_format,
                                        conditions=conditions,
                                        partition_by=partition_by,
                                        **kwargs)
