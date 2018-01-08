
import unittest
from ksql import SQLBuilder
from ksql.error import SQLTypeNotImplementYetError, IllegalTableTypeError, IllegalValueFormatError

class TestSQLBuilder(unittest.TestCase):
	create_table_with_key = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON', key='userid');"
	create_table_without_key = "CREATE TABLE users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');"
	create_stream_without_key = "CREATE STREAM users_original (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');"
				

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
		
		self.assertEqual(build_sql_str.lower(), self.create_table_without_key.lower())

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

		

