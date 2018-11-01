ksql-python
===========

A python wrapper for the KSQL REST API. Easily interact with the KSQL REST API using this library.

Supported KSQL version: 5.x

.. image:: https://travis-ci.org/bryanyang0528/ksql-python.svg?branch=master
  :target: https://travis-ci.org/bryanyang0528/ksql-python

.. image:: https://codecov.io/gh/bryanyang0528/ksql-python/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/bryanyang0528/ksql-python

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

This is the GITHUB page of KSQL. https://github.com/confluentinc/ksql

Setup
~~~~~

-  Setup for the KSQL API:

.. code:: python

    from ksql import KSQLAPI
    client = KSQLAPI('http://ksql-server:8088')

- Setup for KSql API with logging enabled:

.. code:: python

    import logging
    from ksql import KSQLAPI
    logging.basicConfig(level=logging.DEBUG)
    client = KSQLAPI('http://ksql-server:8088')

Options
~~~~~~~

+---------------+-----------+------------+--------------------------------------------------------------+
| Option        | Type      | Required   | Description                                                  |
+===============+===========+============+==============================================================+
| ``url``       | string    | yes        | Your ksql-server url. Example: ``http://ksql-server:8080``   |
+---------------+-----------+------------+--------------------------------------------------------------+
| ``timeout``   | integer   | no         | Timout for Requests. Default: ``5``                          |
+---------------+-----------+------------+--------------------------------------------------------------+

Main Methods
~~~~~~~~~~~~

ksql
^^^^

.. code:: python

    client.ksql('show tables')

-  Example Response ``[{'tables': {'statementText': 'show tables;', 'tables': []}}]``

query
^^^^^

It will execute sql query and keep listening streaming data.

.. code:: python

    client.query('select * from table1')

This command returns a generator. It can be printed e.g. by reading its values via `next(query)` or a for loop:

.. code:: python
    
    for item in query: 
      print(item)


-  Example Response

   ::

       {"row":{"columns":[1512787743388,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753200,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753488,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753888,"key1",1,2,3]},"errorMessage":null}

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
| ``value_format``| string    | no       | ``DELIMITED`` or ``JSON`` (Default)                          |
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

FileUpload
~~~~~~~~~~~~~~

upload
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run commands from a .ksql file. Can only support ksql commands and not streaming queries.

.. code:: python

     from ksql import FileUpload
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
