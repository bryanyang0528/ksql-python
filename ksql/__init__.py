
__package_name__ = "ksql"
__ksql_server_version__ = "4.1.1-SNAPSHOT"
__ksql_api_version__ = "0.1.2"
__version__ = __ksql_server_version__ + "." + __ksql_api_version__

from ksql.client import KSQLAPI
from ksql.builder import SQLBuilder
from ksql.api import SimplifiedAPI
