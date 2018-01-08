
from ksql.error import SQLTypeNotImplementYetError, IllegalTableTypeError, IllegalValueFormatError

class SQLBuilder(object):

	@classmethod
	def build(self, sql_type, **kwargs):
		if sql_type == 'create':
			sql_builder = CreateBuilder(kwargs.pop('table_type'))
			sql_str = sql_builder.build(kwargs.pop('table_name'), 
							  kwargs.pop('columns_type'),
							  kwargs.pop('topic'),
							  kwargs.pop('value_format'),
							  kwargs.pop('key', None))
		else:
			raise SQLTypeNotImplementYetError(sql_type)

		return sql_str

class CreateBuilder(object):
	def __init__(self, table_type):
		self.table_types = ['table', 'stream']
		self.value_formats = ['delimited', 'json']
		self.table_type = table_type

		if table_type.lower() not in self.table_types:
			raise IllegalTableTypeError(table_type)

		self.str_format = "CREATE {} {} ({}) WITH (kafka_topic='{}', value_format='{}'{});"

	def build(self, table_name, columns_type={}, topic=None, value_format='DELIMITED', key=None):
		if value_format.lower() not in self.value_formats:
			raise IllegalValueFormatError(value_format)
			
		built_colums_type = self._build_colums_type(columns_type)
		built_key = self._build_key(key)

		sql_str = self.str_format.format(self.table_type, table_name, built_colums_type, topic, value_format, built_key)
		return sql_str

	@staticmethod
	def _build_colums_type(columns_type):
		built_columns_type = ', '.join(columns_type)
		return built_columns_type

	@staticmethod
	def _build_key(key):
		if key:
			built_key = ", key='{}'".format(key)
		else:
			return ''
		return built_key

