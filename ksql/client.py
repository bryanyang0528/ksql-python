from __future__ import absolute_import
from __future__ import print_function

import json
import requests

from ksql.builder import SQLBuilder
from ksql.error import CreateError


class KSQLAPI(object):
    """ API Class """

    def __init__(self, url, **kwargs):
        self.url = url
        self.timeout = kwargs.get("timeout", 5)

    def get_url(self):
        return self.url

    def get_ksql_version(self):
        r = requests.get(self.url)
        if r.status_code == 200:
            info = r.json().get('KSQL Server Info')
            version = info.get('version')
            return version
        else:
            raise ValueError('Status Code: {}.\nMessage: {}'.format(r.status_code, r.content))

    def _request(self, endpoint, method='post', sql_string=''):
        url = '{}/{}'.format(self.url, endpoint)

        sql_string = self._validate_sql_string(sql_string) 
        data = json.dumps({
            "ksql": sql_string
        })
        
        headers = {
            "Content-Type": "application/json"
        }
        
        if endpoint == 'query':
            stream = True
        else:
            stream = False

        r = requests.request(
            method=method,
            url=url,
            data=data,
            timeout=self.timeout,
            headers=headers,
            stream=stream)  

        return r  

    @staticmethod
    def _validate_sql_string(sql_string):
        if len(sql_string) > 0:
            if sql_string[-1] != ';':
                sql_string += ';'
        return sql_string

    def ksql(self, ksql_string):
        r = self._request(endpoint='ksql', sql_string=ksql_string)

        if r.status_code == 200:
            r = r.json()
            return r
        else: 
            raise ValueError('Status Code: {}.\nMessage: {}'.format(r.status_code, r.content))

    def query(self, query_string, encoding='utf-8', chunk_size=128):
        """  
        Process streaming incoming data.

        """
        r = self._request(endpoint='query', sql_string=query_string)

        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk != b'\n':
                print(chunk.decode(encoding))

    def create_stream(self, table_name, columns_type, topic, value_format):
        return self._create_stream_table(table_type='stream', 
                                    table_name = table_name, 
                                    columns_type = columns_type, 
                                    topic = topic, 
                                    value_format = value_format)

    def create_table(self, table_name, columns_type, topic, value_format):
        return self._create_stream_table(table_type='table', 
                                    table_name = table_name, 
                                    columns_type = columns_type, 
                                    topic = topic, 
                                    value_format = value_format)


    def _create_stream_table(self, table_type, table_name, columns_type, topic, value_format):
        ksql_string = SQLBuilder.build(sql_type = 'create', 
                                      table_type = table_type, 
                                      table_name = table_name, 
                                      columns_type = columns_type, 
                                      topic = topic, 
                                      value_format = value_format)
        r = self.ksql(ksql_string)

        if_current_status = r[0].get('currentStatus')
        if if_current_status:
            r = r[0]['currentStatus']['commandStatus']['status']
        else:
            r = r[0]['error']['errorMessage']['message']

        if r == 'SUCCESS':
            return True
        else:
            raise CreateError(r)
