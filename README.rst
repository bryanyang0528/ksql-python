ksql-python
===========

A python wrapper for the KSQL REST API. Easily interact with the KSQL REST API using this library.

|Build Status|

Installation
------------

.. code:: bash

    pip install ksql-python

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
