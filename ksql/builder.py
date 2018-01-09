
from six import string_types

from ksql.error import SQLTypeNotImplementYetError, \
					   IllegalTableTypeError, \
					   IllegalValueFormatError, \
					   SQLFormatNotImplementError, \
					   BuildNotImplmentError

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

		elif sql_type == 'create_as':
			sql_builder = CreateAsBuilder(kwargs.pop('table_type'))
			sql_str = sql_builder.build(table_name = kwargs.pop('table_name'), 
							  			select_columns = kwargs.pop('select_columns'),
							  			src_table = kwargs.pop('src_table'),
							  			target_topic = kwargs.pop('target_topic'),
							  			value_format = kwargs.pop('value_format'),
							  			partition_by = kwargs.pop('partition_by',''),
							  			**kwargs)

		else:
			raise SQLTypeNotImplementYetError(sql_type)

		return sql_str


class BaseCreateBuilder(object):
	def __init__(self, table_type, sql_format=None):
		self.table_types = ['table', 'stream']
		self.value_formats = ['delimited', 'json']
		self.properties = ['key', 'timestamp']
		self.table_type = table_type
		self.sql_format = sql_format

		if table_type.lower() not in self.table_types:
			raise IllegalTableTypeError(table_type)

		if not sql_format:
			raise SQLFormatNotImplementError()

	def build(self):
		raise BuildNotImplmentError()

	def _parsed_with_properties(self, **kwargs):
		properties = ['']
		for prop in self.properties:
			if prop in kwargs.keys():
				value = kwargs.get(prop)
				if isinstance(value, string_types):
					p = "{}='{}'".format(prop, kwargs.get(prop))
				else:
					p = "{}={}".format(prop, kwargs.get(prop))
				
				properties.append(p)

		return ', '.join(properties)


class CreateBuilder(BaseCreateBuilder):
	def __init__(self, table_type):
		str_format = "CREATE {} {} ({}) WITH (kafka_topic='{}', value_format='{}'{});"
		super(CreateBuilder, self).__init__(table_type, str_format)

	def build(self, table_name, columns_type=[], topic=None, value_format='DELIMITED', key=None):
		if value_format.lower() not in self.value_formats:
			raise IllegalValueFormatError(value_format)
			
		built_colums_type = self._build_colums_type(columns_type)
		built_key = self._build_key(key)

		sql_str = self.sql_format.format(self.table_type, table_name, built_colums_type, topic, value_format, built_key)
		return sql_str

	@staticmethod
	def _build_colums_type(columns_type):
		built_columns_type = ', '.join(columns_type)
		return built_columns_type

	@staticmethod
	def _build_key(key):
		if key:
			built_key = ", key='{}'".format(key)
			return built_key
		else:
			return ''


class CreateAsBuilder(BaseCreateBuilder):
	def __init__(self, table_type):
		str_format = "CREATE {} {} WITH (kafka_topic='{}', value_format='{}'{}) AS SELECT {} FROM {} {}"
		super(CreateAsBuilder, self).__init__(table_type, str_format)
		
	def build(self, table_name, select_columns, src_table, target_topic=None, 
			  value_format='DELIMITED', conditions=[], partition_by='', **kwargs):

		if value_format.lower() not in self.value_formats:
			raise IllegalValueFormatError(value_format)
		
		if not target_topic:
			target_topic = table_name

		select_clause = self._build_select_clause(select_columns)
		where_clause = self._build_where_clause(conditions)
		properties = self._parsed_with_properties(**kwargs)
		print(properties)

		sql_str = self.sql_format.format(self.table_type, table_name, target_topic, value_format, properties, select_clause, src_table, where_clause, partition_by)
		return sql_str.strip()

	@staticmethod
	def _build_where_clause(conditions):
		if len(conditions) > 0:
			where_clause = "where {}".format(conditions)
			return where_clause
		else:
			return ''

	@staticmethod
	def _build_select_clause(select_columns):
		if len(select_columns) > 0:
			return ', '.join(select_columns)
		else:
			return '*'


		
