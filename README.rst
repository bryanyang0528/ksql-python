ksql-python
===========

A python wrapper for the KSQL REST API. Easily interact with the KSQL REST API using this library.

Supported KSQLDB version: 0.10.1+
Supported Python version: 3.5+

.. image:: https://travis-ci.org/bryanyang0528/ksql-python.svg?branch=master
  :target: https://travis-ci.org/bryanyang0528/ksql-python

.. image:: https://codecov.io/gh/bryanyang0528/ksql-python/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/bryanyang0528/ksql-python

.. image:: https://pepy.tech/badge/ksql
  :target: https://pepy.tech/project/ksql

.. image:: https://pepy.tech/badge/ksql/month
  :target: https://pepy.tech/project/ksql/month
  
.. image:: https://img.shields.io/badge/license-MIT-yellow.svg
  :target: https://github.com/bryanyang0528/ksql-python/blob/master/LICENSE  
  
Installation
------------

.. code:: bash

    pip install ksql

Or

.. code:: bash

    git clone https://github.com/bryanyang0528/ksql-python
    cd ksql-python
    python setup.py install

Getting Started
---------------

Setup for KSQL
~~~~~~~~~~~~~~~

This is the GITHUB page of KSQL. https://github.com/confluentinc/ksql

If you have installed open source Confluent CLI (e.g. by installing Confluent Open Source or Enterprise Platform), you can start KSQL and its dependencies with one single command:

.. code:: bash

    confluent start ksql-server

Setup for ksql-python API
~~~~~~~~~~~~~~~~~~~~~~~~~

-  Setup for the KSQL API:

.. code:: python

    from ksql import KSQLAPI
    client = KSQLAPI('http://ksql-server:8088')

- Setup for KSQl API with logging enabled:

.. code:: python

    import logging
    from ksql import KSQLAPI
    logging.basicConfig(level=logging.DEBUG)
    client = KSQLAPI('http://ksql-server:8088')

- Setup for KSQL API with Basic Authentication

.. code:: python

    from ksql import KSQLAPI
    client = KSQLAPI('http://ksql-server:8088', api_key="your_key", secret="your_secret")

Options
~~~~~~~

+---------------+-----------+------------+--------------------------------------------------------------+
| Option        | Type      | Required   | Description                                                  |
+===============+===========+============+==============================================================+
| ``url``       | string    | yes        | Your ksql-server url. Example: ``http://ksql-server:8080``   |
+---------------+-----------+------------+--------------------------------------------------------------+
| ``timeout``   | integer   | no         | Timout for Requests. Default: ``5``                          |
+---------------+-----------+------------+--------------------------------------------------------------+
| ``api_key``   | string    | no         | API Key to use on the requests                               |
+---------------+-----------+------------+--------------------------------------------------------------+
| ``secret``    | string    | no         | Secret to use on the requests                                |
+---------------+-----------+------------+--------------------------------------------------------------+

Main Methods
~~~~~~~~~~~~

ksql
^^^^

This method can be used for some KSQL features which are not supported via other specific methods like ``query``, ``create_stream`` or ``create_stream_as``.
The following example shows how to execute the ``show tables`` statement:

.. code:: python

    client.ksql('show tables')

-  Example Response ``[{'tables': {'statementText': 'show tables;', 'tables': []}}]``

query
^^^^^

It will execute sql query and keep listening streaming data.

.. code:: python

    client.query('select * from table1')

This command returns a generator. It can be printed e.g. by reading its values via `next(query)` or a for loop. Here is a complete example:

.. code:: python
    
  from ksql import KSQLAPI
  client = KSQLAPI('http://localhost:8088')
  query = client.query('select * from table1')
  for item in query: print(item)

-  Example Response

   ::

       {"row":{"columns":[1512787743388,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753200,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753488,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753888,"key1",1,2,3]},"errorMessage":null}

Query with HTTP/2
^^^^^^^^^^^^^^^^^
Execute queries with the new ``/query-stream`` endpoint. Documented `here <https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-rest-api/streaming-endpoint/#executing-pull-or-push-queries>`_

To execute a sql query use the same syntax as the regular query, with the additional ``use_http2=True`` parameter.

.. code:: python

    client.query('select * from table1', use_http2=True)

A generator is returned with the following example response

   ::

       {"queryId":"44d8413c-0018-423d-b58f-3f2064b9a312","columnNames":["ORDER_ID","TOTAL_AMOUNT","CUSTOMER_NAME"],"columnTypes":["INTEGER","DOUBLE","STRING"]}
       [3,43.0,"Palo Alto"]
       [3,43.0,"Palo Alto"]
       [3,43.0,"Palo Alto"]

To terminate the query above use the ``close_query`` call.
Provide the ``queryId`` returned from the ``query`` call.

.. code:: python

    client.close_query("44d8413c-0018-423d-b58f-3f2064b9a312")

Insert rows into a Stream with HTTP/2
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Uses the new ``/inserts-stream`` endpoint. See `documentation <https://docs.ksqldb.io/en/0.10.0-ksqldb/developer-guide/ksqldb-rest-api/streaming-endpoint/#inserting-rows-into-an-existing-stream>`_

.. code:: python

    rows = [
            {"ORDER_ID": 1, "TOTAL_AMOUNT": 23.5, "CUSTOMER_NAME": "abc"},
            {"ORDER_ID": 2, "TOTAL_AMOUNT": 3.7, "CUSTOMER_NAME": "xyz"}
        ]

    results = self.api_client.inserts_stream("my_stream_name", rows)

An array of object will be returned on success, with the status of each row inserted.


Simplified API
~~~~~~~~~~~~~~

create_stream/ create_table
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    client.create_stream(table_name=table_name,
                         columns_type=columns_type,
                         topic=topic,
                         value_format=value_format)

Options
^^^^^^^

+-----------------+-----------+----------+--------------------------------------------------------------+
| Option          | Type      | Required | Description                                                  |
+=================+===========+==========+==============================================================+
| ``table_name``  | string    | yes      | name of stream/table                                         |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``columns_type``| list      | yes      | ex:``['viewtime bigint','userid varchar','pageid varchar']`` |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``topic``       | string    | yes      | Kafka topic                                                  |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``value_format``| string    | no       | ``JSON`` (Default) or ``DELIMITED`` or ``AVRO``              |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``key``         | string    | for Table| Key (used for JOINs)                                         |
+-----------------+-----------+----------+--------------------------------------------------------------+


-  Responses

:If create table/stream succeed:
  return True

:If failed:
  raise a CreateError(respose_from_ksql_server)

create_stream_as
^^^^^^^^^^^^^^^^

a simplified api for creating stream as select

.. code:: python

    client.create_stream_as(table_name=table_name,
                            select_columns=select_columns,
                            src_table=src_table,
                            kafka_topic=kafka_topic,
                            value_format=value_format,
                            conditions=conditions,
                            partition_by=partition_by,
                            **kwargs)


.. code:: sql

  CREATE STREAM <table_name>
  [WITH ( kafka_topic=<kafka_topic>, value_format=<value_format>, property_name=expression ... )]
  AS SELECT  <select_columns>
  FROM <src_table>
  [WHERE <conditions>]
  PARTITION BY <partition_by>];

Options
^^^^^^^

+-------------------+-----------+----------+--------------------------------------------------------------+
| Option            | Type      | Required | Description                                                  |
+===================+===========+==========+==============================================================+
| ``table_name``    | string    | yes      | name of stream/table                                         |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``select_columns``| list      | yes      | you can select ``[*]`` or ``['columnA', 'columnB']``         |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``src_table``     | string    | yes      | name of source table                                         |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``kafka_topic``   | string    | no       | The name of the Kafka topic of this new stream(table).       |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``value_format``  | string    | no       | ``DELIMITED``, ``JSON``(Default) or ``AVRO``                 |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``conditions``    | string    | no       | The conditions in the where clause.                          |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``partition_by``  | string    | no       | Data will be distributed across partitions by this column.   |
+-------------------+-----------+----------+--------------------------------------------------------------+
| ``kwargs``        | pair      | no       | please provide ``key=value`` pairs. Please see more options. |
+-------------------+-----------+----------+--------------------------------------------------------------+

KSQL JOINs
~~~~~~~~~~~~~~

KSQL JOINs between Streams and Tables are not supported yet via explicit methods, but you can use the ``ksql`` method for this like the following:

.. code:: python

    client.ksql("CREATE STREAM join_per_user WITH (VALUE_FORMAT='AVRO', KAFKA_TOPIC='join_per_user') AS SELECT Time, Amount FROM source c INNER JOIN users u on c.user = u.userid WHERE u.USERID = 1")

FileUpload
~~~~~~~~~~~~~~

upload
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run commands from a .ksql file. Can only support ksql commands and not streaming queries.

.. code:: python

     from ksql.upload import FileUpload
     pointer = FileUpload('http://ksql-server:8080')
     pointer.upload('rules.ksql')


Options
^^^^^^^

+-----------------+-----------+----------+--------------------------------------------------------------+
| Option          | Type      | Required | Description                                                  |
+=================+===========+==========+==============================================================+
| ``ksqlfile``    | string    | yes      | name of file containing the rules                            |
+-----------------+-----------+----------+--------------------------------------------------------------+


-  Responses

:If ksql-commands succesfully executed:
  return (List of server response for all commands)

:If failed:
  raise the appropriate error

More Options
^^^^^^^^^^^^

There are more properties (partitions, replicas, etc...) in the official document.

`KSQL Syntax Reference <https://github.com/confluentinc/ksql/blob/0.1.x/docs/syntax-reference.md#syntax-reference>`_

-  Responses

:If create table/stream succeed:
  return True

:If failed:
  raise a CreatError(respose_from_ksql_server)
