import time

import base64
import functools
import json
import logging
import requests
import urllib
from copy import deepcopy
from requests import Timeout

from ksql.builder import SQLBuilder
from ksql.errors import CreateError, InvalidQueryError, KSQLError


class BaseAPI(object):
    def __init__(self, url, **kwargs):
        self.url = url
        self.max_retries = kwargs.get("max_retries", 3)
        self.delay = kwargs.get("delay", 0)
        self.timeout = kwargs.get("timeout", 15)
        self.api_key = kwargs.get("api_key")
        self.secret = kwargs.get("secret")
        self.headers = {
            'Content-Type': 'application/vnd.ksql.v1+json; charset=utf-8',
        }

    def get_timout(self):
        return self.timeout

    @staticmethod
    def _validate_sql_string(sql_string):
        if len(sql_string) > 0:
            if sql_string[-1] != ";":
                sql_string = sql_string + ";"
        else:
            raise InvalidQueryError(sql_string)
        return sql_string

    @staticmethod
    def _raise_for_status(r, response):
        r_json = json.loads(response)
        if r.getcode() != 200:
            # seems to be the new API behavior
            if r_json.get("@type") == "statement_error" or r_json.get("@type") == "generic_error":
                error_message = r_json["message"]
                error_code = r_json["error_code"]
                stackTrace = r_json["stack_trace"]
                raise KSQLError(error_message, error_code, stackTrace)
            else:
                raise KSQLError("Unknown Error: {}".format(r.content))
        else:
            # seems to be the old API behavior, so some errors have status 200, bug??
            if r_json[0]["@type"] == "currentStatus" and r_json[0]["commandStatus"]["status"] == "ERROR":
                error_message = r_json[0]["commandStatus"]["message"]
                error_code = None
                stackTrace = None
                raise KSQLError(error_message, error_code, stackTrace)
            return True

    def ksql(self, ksql_string, stream_properties=None):
        r = self._request(endpoint="ksql", sql_string=ksql_string, stream_properties=stream_properties)
        response = r.read().decode("utf-8")
        self._raise_for_status(r, response)
        res = json.loads(response)
        return res

    def query(self, query_string, encoding="utf-8", chunk_size=128, stream_properties=None, idle_timeout=None):
        """
        Process streaming incoming data.

        """
        streaming_response = self._request(
            endpoint="query", sql_string=query_string, stream_properties=stream_properties
        )
        start_idle = None

        if streaming_response.code == 200:
            for chunk in streaming_response:
                if chunk != b"\n":
                    start_idle = None
                    yield chunk.decode(encoding)
                else:
                    if not start_idle:
                        start_idle = time.time()
                    if idle_timeout and time.time() - start_idle > idle_timeout:
                        print("Ending query because of time out! ({} seconds)".format(idle_timeout))
                        return
        else:
            raise ValueError("Return code is {}.".format(streaming_response.status_code))

    def get_request(self, endpoint):
        auth = (self.api_key, self.secret) if self.api_key or self.secret else None
        return requests.get(endpoint, headers=self.headers, auth=auth)

    def _request(self, endpoint, method="POST", sql_string="", stream_properties=None, encoding="utf-8"):
        url = "{}/{}".format(self.url, endpoint)

        logging.debug("KSQL generated: {}".format(sql_string))

        sql_string = self._validate_sql_string(sql_string)
        body = {"ksql": sql_string}
        if stream_properties:
            body["streamsProperties"] = stream_properties
        else:
            body["streamsProperties"] = {}
        data = json.dumps(body).encode(encoding)

        headers = deepcopy(self.headers)
        if self.api_key and self.secret:
            base64string = base64.b64encode(bytes("{}:{}".format(self.api_key, self.secret), "utf-8")).decode("utf-8")
            headers["Authorization"] = "Basic %s" % base64string

        req = urllib.request.Request(url=url, data=data, headers=headers, method=method.upper())

        try:
            r = urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as http_error:
            try:
                content = json.loads(http_error.read().decode(encoding))
            except Exception as e:
                raise http_error
            else:
                logging.debug("content: {}".format(content))
                raise KSQLError(e=content.get("message"), error_code=content.get("error_code"))
        else:
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

    def create_stream(self, table_name, columns_type, topic, value_format="JSON"):
        return self._create(
            table_type="stream",
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
        )

    def create_table(self, table_name, columns_type, topic, value_format, key):
        if not key:
            raise ValueError("key is required for creating a table.")
        return self._create(
            table_type="table",
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
            key=key,
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
        return self._create_as(
            table_type="stream",
            table_name=table_name,
            select_columns=select_columns,
            src_table=src_table,
            kafka_topic=kafka_topic,
            value_format=value_format,
            conditions=conditions,
            partition_by=partition_by,
            **kwargs,
        )

    def _create(self, table_type, table_name, columns_type, topic, value_format="JSON", key=None):
        ksql_string = SQLBuilder.build(
            sql_type="create",
            table_type=table_type,
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
            key=key,
        )
        self.ksql(ksql_string)
        return True

    @BaseAPI.retry(exceptions=(Timeout, CreateError))
    def _create_as(
        self,
        table_type,
        table_name,
        select_columns,
        src_table,
        kafka_topic=None,
        value_format="JSON",
        conditions=[],
        partition_by=None,
        **kwargs
    ):
        ksql_string = SQLBuilder.build(
            sql_type="create_as",
            table_type=table_type,
            table_name=table_name,
            select_columns=select_columns,
            src_table=src_table,
            kafka_topic=kafka_topic,
            value_format=value_format,
            conditions=conditions,
            partition_by=partition_by,
            **kwargs,
        )
        self.ksql(ksql_string)
        return True
