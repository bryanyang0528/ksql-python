import functools
import json
import threading
import time
import logging

import requests
from requests import Timeout

from ksql.builder import SQLBuilder
from ksql.errors import CreateError, KSQLError, InvalidQueryError


class BaseAPI(object):
    def __init__(self, url, **kwargs):
        self.url = url
        self.max_retries = kwargs.get("max_retries", 3)
        self.delay = kwargs.get("delay", 0)
        self.timeout = kwargs.get("timeout", 15)

    def get_timout(self):
        return self.timeout

    @staticmethod
    def _validate_sql_string(sql_string):
        if len(sql_string) > 0:
            if sql_string[-1] != ';':
                sql_string = sql_string + ';'
        else:
            raise InvalidQueryError(sql_string)
        return sql_string

    @staticmethod
    def _raise_for_status(r):
        try:
            r_json = r.json()
        except ValueError:
            r.raise_for_status()
        if r.status_code != 200:
            # seems to be the new API behavior
            if r_json.get('@type') == 'statement_error' or r_json.get('@type') == 'generic_error':
                error_message = r_json['message']
                error_code = r_json['error_code']
                stackTrace = r_json['stackTrace']
                raise KSQLError(error_message, error_code, stackTrace)
            else:
                raise KSQLError("Unknown Error: {}".format(r.content))
        else:
            # seems to be the old API behavior, so some errors have status 200, bug??
            if r_json[0]['@type'] == 'currentStatus' \
                    and r_json[0]['commandStatus']['status'] == 'ERROR':
                error_message = r_json[0]['commandStatus']['message']
                error_code = None
                stackTrace = None
                raise KSQLError(error_message, error_code, stackTrace)
            return True

    def ksql(self, ksql_string, stream_properties=None):
        r = self._request(endpoint='ksql', sql_string=ksql_string, stream_properties=stream_properties)
        self._raise_for_status(r)
        r = r.json()
        return r

    def query(self, query_string, encoding='utf-8', chunk_size=128, stream_properties=None, idle_timeout=None):
        """
        Process streaming incoming data.

        """
        streaming_response = self._request(endpoint='query', sql_string=query_string, stream_properties=stream_properties)
        start_idle = None
        for chunk in streaming_response.iter_content(chunk_size=chunk_size):
            if chunk != b'\n':
                start_idle = None
                yield chunk.decode(encoding)
            else:
                if not start_idle:
                    start_idle = time.time()
                if idle_timeout and time.time() - start_idle > idle_timeout:
                    print('Ending query because of time out! ({} seconds)'.format(idle_timeout))
                    return

    def _request(self, endpoint, method='post', sql_string='', stream_properties=None):
        url = '{}/{}'.format(self.url, endpoint)

        logging.debug("KSQL generated: {}".format(sql_string))

        sql_string = self._validate_sql_string(sql_string)
        body = {
            "ksql": sql_string
        }
        if stream_properties:
            body['streamsProperties'] = stream_properties
        data = json.dumps(body)

        headers = {
            "Accept": "application/json",
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
    def retry(exceptions, delay=1, max_retries=5):
        """
        A decorator for retrying a function call with a specified delay in case of a set of exceptions

        Parameter List
        -------------
        :param exceptions:  A tuple of all exceptions that need to be caught for retry
                                            e.g. retry(exception_list = (Timeout, Readtimeout))
        :param delay: Amount of delay (seconds) needed between successive retries.
        :param times: no of times the function should be retried

        """

        def outer_wrapper(function):
            @functools.wraps(function)
            def inner_wrapper(*args, **kwargs):
                final_excep = None
                for counter in range(max_retries):
                    if counter > 0:
                        time.sleep(delay)
                    final_excep = None
                    try:
                        value = function(*args, **kwargs)
                        return value
                    except (exceptions) as e:
                        final_excep = e
                        pass  # or log it

                if final_excep is not None:
                    raise final_excep

            return inner_wrapper

        return outer_wrapper


class SimplifiedAPI(BaseAPI):
    def __init__(self, url, **kwargs):
        super(SimplifiedAPI, self).__init__(url, **kwargs)

    def create_stream(
            self,
            table_name,
            columns_type,
            topic,
            value_format='JSON'):
        return self._create(table_type='stream',
                            table_name=table_name,
                            columns_type=columns_type,
                            topic=topic,
                            value_format=value_format)

    def create_table(self, table_name, columns_type, topic, value_format, key):
        if not key:
            raise ValueError('key is required for creating a table.')
        return self._create(table_type='table',
                            table_name=table_name,
                            columns_type=columns_type,
                            topic=topic,
                            value_format=value_format,
                            key=key)

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
        return self._create_as(table_type='stream',
                               table_name=table_name,
                               select_columns=select_columns,
                               src_table=src_table,
                               kafka_topic=kafka_topic,
                               value_format=value_format,
                               conditions=conditions,
                               partition_by=partition_by,
                               **kwargs)

    def _create(
            self,
            table_type,
            table_name,
            columns_type,
            topic,
            value_format='JSON',
            key=None):
        ksql_string = SQLBuilder.build(sql_type='create',
                                       table_type=table_type,
                                       table_name=table_name,
                                       columns_type=columns_type,
                                       topic=topic,
                                       value_format=value_format,
                                       key=key)
        r = self.ksql(ksql_string)
        return True

    @BaseAPI.retry(exceptions=(Timeout, CreateError))
    def _create_as(
            self,
            table_type,
            table_name,
            select_columns,
            src_table,
            kafka_topic=None,
            value_format='JSON',
            conditions=[],
            partition_by=None,
            **kwargs):
        ksql_string = SQLBuilder.build(sql_type='create_as',
                                       table_type=table_type,
                                       table_name=table_name,
                                       select_columns=select_columns,
                                       src_table=src_table,
                                       kafka_topic=kafka_topic,
                                       value_format=value_format,
                                       conditions=conditions,
                                       partition_by=partition_by,
                                       **kwargs)
        r = self.ksql(ksql_string)
        return True
