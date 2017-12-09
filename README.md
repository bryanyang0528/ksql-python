# ksql-python

A python wrapper for the KSql REST API. Easily interact with the KSql REST API using this library.

[![Build Status](https://travis-ci.org/bryanyang0528/ksql-python.svg?branch=master)](https://travis-ci.org/bryanyang0528/ksql-python)

## Installation

```bash
git clone ksql-python
cd ksql-python
python setup.py install
```

## Getting Started

This is the GITHUB page of KSQL.
[https://github.com/confluentinc/ksql](https://github.com/confluentinc/ksql)

### Setup

* Setup for the KSql API:

```python
from ksql import KSqlAPI
client = KSqlAPI('http://ksql-server:8080')
```

### Options

| Option        | Type           | Required  | Description|
| ------------- |:-------------:| -----:|-------:|
| `url`      | string | yes | Your ksql-server url. Example: `http://ksql-server:8080` |
| `timeout`     | integer | no | Timout for Requests. Default: `5`|

### Main Methods

#### ksql
```python
client.ksql('show tables')
```
* Example Response
`[{'tables': {'statementText': 'show tables;', 'tables': []}}]`



#### query
It will execute sql query and keep listening streaming data.
```python
client.query('select * from table1')
```
* Example Response
```
{"row":{"columns":[1512787743388,"key1",1,2,3]},"errorMessage":null}
{"row":{"columns":[1512787753200,"key1",1,2,3]},"errorMessage":null}
{"row":{"columns":[1512787753488,"key1",1,2,3]},"errorMessage":null}
{"row":{"columns":[1512787753888,"key1",1,2,3]},"errorMessage":null}
```