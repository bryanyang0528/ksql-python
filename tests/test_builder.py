
import unittest
from ksql import SQLBuilder
from ksql.error import SQLTypeNotImplementYetError, \
					   IllegalTableTypeError, \
					   IllegalValueFormatError

class TestSQLBuilder(unittest.TestCase):
	create_table_with_key = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON', key='userid');"
	create_table_without_key = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');"
	create_table_without_key_avro = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='AVRO');"
	create_table_without_key_delimited = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='DELIMITED');"
	create_stream_without_key = "CREATE STREAM users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');"
	create_stream_as_with_condition = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', timestamp='logtime') AS SELECT rowtime as logtime, * FROM pageviews_original WHERE userid like 'User_%' AND pageid like 'Page_%'"	
	create_stream_as_with_condition_select_star = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', timestamp='logtime') AS SELECT * FROM pageviews_original WHERE userid like 'User_%' AND pageid like 'Page_%'"	
	create_stream_as_without_condition = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', timestamp='logtime') AS SELECT rowtime as logtime, * FROM pageviews_original"	
	create_stream_as_without_condition_select_star = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', timestamp='logtime') AS SELECT * FROM pageviews_original"	
	create_stream_as_with_condition_with_partitions = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', partitions=5, timestamp='logtime') AS SELECT rowtime as logtime, * FROM pageviews_original WHERE userid like 'User_%' AND pageid like 'Page_%'"	
	create_stream_as_without_condition_partition_by = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='DELIMITED', timestamp='logtime') AS SELECT rowtime as logtime, * FROM pageviews_original PARTITION BY logtime"	
	create_stream_as_without_condition_avro = "CREATE STREAM pageviews_valid WITH (kafka_topic='pageviews_valid', value_format='AVRO', timestamp='logtime') AS SELECT rowtime as logtime, * FROM pageviews_original"	
	

	def test_create_table_with_key(self):
		
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'JSON'
		key = 'userid'
		
		build_sql_str = SQLBuilder.build('create', 
										table_type = 'table', 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format, 
										key = key)
		
		self.assertEqual(build_sql_str.lower(), self.create_table_with_key.lower())

	def test_create_table_without_key(self):
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'JSON'
		
		build_sql_str = SQLBuilder.build('create', 
										table_type = 'table', 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)

	def test_create_table_without_key_avro(self):
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'AVRO'
		
		build_sql_str = SQLBuilder.build('create', 
										table_type = 'table', 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)
		
		self.assertEqual(build_sql_str.lower(), self.create_table_without_key_avro.lower())

	def test_create_table_without_key_delimited(self):
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'DELIMITED'
		
		build_sql_str = SQLBuilder.build('create', 
										table_type = 'table', 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)
		
		self.assertEqual(build_sql_str.lower(), self.create_table_without_key_delimited.lower())


	def test_create_stream_without_key(self):
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'JSON'
		
		build_sql_str = SQLBuilder.build('create', 
										table_type = 'stream', 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)
		
		self.assertEqual(build_sql_str.lower(), self.create_stream_without_key.lower())

	def test_create_stream_as_without_condition(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['rowtime as logtime', '*']
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										select_columns = select_columns,
										timestamp='logtime', 
										value_format = value_format)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition.lower())

	def test_create_stream_as_without_condition_avro(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'AVRO'
		select_columns = ['rowtime as logtime', '*']
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										select_columns = select_columns,
										timestamp='logtime', 
										value_format = value_format)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition_avro.lower())

	def test_create_stream_as_without_condition_select_star(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										timestamp='logtime', 
										value_format = value_format)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition_select_star.lower())

	def test_create_stream_as_without_condition_select_star_with_blank_list(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = []
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										timestamp='logtime', 
										value_format = value_format,
										select_columns = select_columns)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition_select_star.lower())

	def test_create_stream_as_without_condition_select_star_with_only_star(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['*']
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										timestamp='logtime', 
										value_format = value_format,
										select_columns = select_columns)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition_select_star.lower())

	def test_create_stream_as_with_condition(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['rowtime as logtime', '*']
		conditions = "userid like 'User_%' AND pageid like 'Page_%'"
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										select_columns = select_columns,
										timestamp='logtime', 
										value_format = value_format,
										conditions = conditions)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_with_condition.lower())

	def test_create_stream_as_with_condition_double_qoute(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['rowtime as logtime', '*']
		conditions = 'userid like "User_%" AND pageid like "Page_%"'
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										select_columns = select_columns,
										timestamp='logtime', 
										value_format = value_format,
										conditions = conditions)

		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_with_condition.lower())

	def test_create_stream_as_with_condition_with_partitions(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['rowtime as logtime', '*']
		conditions = "userid like 'User_%' AND pageid like 'Page_%'"
		paritions = 5
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = 'stream', 
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic, 
										select_columns = select_columns,
										timestamp='logtime', 
										value_format = value_format,
										conditions = conditions,
										partitions=paritions)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_with_condition_with_partitions.lower())

	def test_create_stream_as_without_condition_partition_by(self):
		sql_type = 'create_as'
		table_name = 'pageviews_valid'
		src_table = 'pageviews_original'
		kafka_topic = 'pageviews_valid'
		value_format = 'DELIMITED'
		select_columns = ['rowtime as logtime', '*']
		partition_by = 'logtime'
		
		built_sql_str = SQLBuilder.build(sql_type = sql_type,
										table_type = 'stream',
										table_name = table_name,
										src_table =   src_table,
										kafka_topic = kafka_topic,
										select_columns = select_columns,
										timestamp='logtime',
										value_format = value_format,
										partition_by = partition_by)
		
		self.assertEqual(built_sql_str.lower(), 
			self.create_stream_as_without_condition_partition_by.lower())

	def test_sql_type_error(self):
		sql_type = 'view'
		table_type = 'stream'
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'JSON'
		
		with self.assertRaises(SQLTypeNotImplementYetError):
			build_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = table_type, 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)

	def test_table_type_error(self):
		sql_type = 'create'
		table_type = 'qoo'
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'JSON'
		
		with self.assertRaises(IllegalTableTypeError):
			build_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = table_type, 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)

	def test_value_format_error(self):
		sql_type = 'create'
		table_type = 'stream'
		table_name = 'users_original'
		columns_type = ['registertime bigint',
						'gender varchar',
						'regionid varchar',
						'userid varchar']
		topic = 'users'
		value_format = 'foo'
		
		with self.assertRaises(IllegalValueFormatError):
			build_sql_str = SQLBuilder.build(sql_type = sql_type, 
										table_type = table_type, 
										table_name = table_name, 
										columns_type = columns_type, 
										topic = topic, 
										value_format = value_format)

