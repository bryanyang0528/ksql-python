ksql-python
===========

A python wrapper for the KSQL REST API. Easily interact with the KSQL REST API using this library.

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

-  Setup for the KSql API:

.. code:: python

    from ksql import KSQLAPI
    client = KSQLAPI('http://ksql-server:8080')

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

-  Example Response

   ::

       {"row":{"columns":[1512787743388,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753200,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753488,"key1",1,2,3]},"errorMessage":null}
       {"row":{"columns":[1512787753888,"key1",1,2,3]},"errorMessage":null}

.. |Build Status| image:: https://travis-ci.org/bryanyang0528/ksql-python.svg?branch=master
   :target: https://travis-ci.org/bryanyang0528/ksql-python


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
| ``table_name``  | string    | yes      | Your ksql-server url. Example: ``http://ksql-server:8080``   |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``columns_type``| list      | yes      | ex:``['viewtime bigint','userid varchar','pageid varchar']`` |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``topic``       | string    | yes      | Kafka topic                                                  |
+-----------------+-----------+----------+--------------------------------------------------------------+
| ``value_format``| string    | no       | ``DELIMITED``(Default) or ``JSON``                           |
+-----------------+-----------+----------+--------------------------------------------------------------+

-  Example Response ``{string: '[{"currentStatus":{"statementText":"CREATE STREAM test_table (viewtime
        bigint, userid varchar, pageid varchar) WITH (kafka_topic=''t1'',
        value_format=''DELIMITED'');","commandId":"stream/TEST_TABLE","commandStatus":{"status":"SUCCESS","message":"Stream
        created"}}}]'}`` 
