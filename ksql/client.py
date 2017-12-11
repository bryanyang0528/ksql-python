from __future__ import absolute_import
from __future__ import print_function

import json
import requests

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

    def __request(self, endpoint, method='post', sql_string=''):
        url = '{}/{}'.format(self.url, endpoint)

        sql_string = self.__validate_sql_string(sql_string) 
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
    def __validate_sql_string(sql_string):
        if len(sql_string) > 0:
            if sql_string[-1] != ';':
                sql_string += ';'
        return sql_string

    def ksql(self, ksql_string):
        r = self.__request(endpoint='ksql', sql_string=ksql_string)

        if r.status_code == 200:
            r = r.json()
            return r
        else: 
            raise ValueError('Status Code: {}.\nMessage: {}'.format(r.status_code, r.content))

    def query(self, query_string, encoding='utf-8', chunk_size=128):
        """  
        Process streaming incoming data.

        """
        r = self.__request(endpoint='query', sql_string=query_string)

        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk != b'\n':
                print(chunk.decode(encoding))
