import re

from six import string_types

from ksql.errors import (
    SQLTypeNotImplementYetError,
    IllegalTableTypeError,
    IllegalValueFormatError,
    SQLFormatNotImplementError,
    BuildNotImplmentError,
)


class SQLBuilder(object):
    @classmethod
    def build(self, sql_type, **kwargs):
        if sql_type == "create":
            sql_builder = CreateBuilder(kwargs.pop("table_type"))
            sql_str = sql_builder.build(
                kwargs.pop("table_name"),
                kwargs.pop("columns_type"),
                kwargs.pop("topic"),
                kwargs.pop("value_format", "JSON"),
                kwargs.pop("key", None),
            )

        elif sql_type == "create_as":
            sql_builder = CreateAsBuilder(kwargs.pop("table_type"))
            sql_str = sql_builder.build(
                table_name=kwargs.pop("table_name"),
                select_columns=kwargs.pop("select_columns", None),
                src_table=kwargs.pop("src_table"),
                kafka_topic=kwargs.pop("kafka_topic"),
                value_format=kwargs.pop("value_format"),
                partition_by=kwargs.pop("partition_by", None),
                **kwargs,
            )

        else:
            raise SQLTypeNotImplementYetError(sql_type)

        return sql_str


class BaseCreateBuilder(object):
    def __init__(self, table_type, sql_format=None):
        self.table_types = ["table", "stream"]
        self.value_formats = ["delimited", "json", "avro"]
        self.table_type = table_type
        self.sql_format = sql_format

        if table_type.lower() not in self.table_types:
            raise IllegalTableTypeError(table_type)

        if not sql_format:
            raise SQLFormatNotImplementError()

    def build(self):
        raise BuildNotImplmentError()

    def _parsed_with_properties(self, **kwargs):
        properties = [""]
        for prop in self.properties:
            if prop in kwargs.keys():
                value = kwargs.get(prop)
                if isinstance(value, string_types):
                    p = "{}='{}'".format(prop, kwargs.get(prop))
                else:
                    p = "{}={}".format(prop, kwargs.get(prop))

                properties.append(p)

        return ", ".join(properties)


class CreateBuilder(BaseCreateBuilder):
    def __init__(self, table_type):
        str_format = "CREATE {} {} ({}) WITH (kafka_topic='{}', value_format='{}'{});"
        super(CreateBuilder, self).__init__(table_type, str_format)
        self.properties = ["kafka_topic", "value_format", "key", "timestamp"]

    def build(self, table_name, columns_type=[], topic=None, value_format="JSON", key=None):
        if value_format.lower() not in self.value_formats:
            raise IllegalValueFormatError(value_format)

        built_colums_type = self._build_colums_type(columns_type)
        built_key = self._build_key(key)

        sql_str = self.sql_format.format(self.table_type, table_name, built_colums_type, topic, value_format, built_key)
        return sql_str

    @staticmethod
    def _build_colums_type(columns_type):
        built_columns_type = ", ".join(columns_type)
        return built_columns_type

    @staticmethod
    def _build_key(key):
        if key:
            built_key = ", key='{}'".format(key)
            return built_key
        else:
            return ""


class CreateAsBuilder(BaseCreateBuilder):
    def __init__(self, table_type):
        str_format = "CREATE {} {} WITH (kafka_topic='{}', value_format='{}'{}) AS SELECT {} FROM {} {} {}"
        super(CreateAsBuilder, self).__init__(table_type, str_format)
        self.properties = ["kafka_topic", "value_format", "partitions", "replicas", "timestamp"]

    def build(
        self,
        table_name,
        select_columns,
        src_table,
        kafka_topic=None,
        value_format="JSON",
        conditions=[],
        partition_by=None,
        **kwargs
    ):

        if value_format.lower() not in self.value_formats:
            raise IllegalValueFormatError(value_format)

        if not kafka_topic:
            kafka_topic = table_name

        select_clause, where_clause, partition_by_clause, properties = self._build_clauses(
            select_columns, conditions, partition_by, **kwargs
        )

        sql_str = self.sql_format.format(
            self.table_type,
            table_name,
            kafka_topic,
            value_format,
            properties,
            select_clause,
            src_table,
            where_clause,
            partition_by_clause,
        )

        cleaned_sql_str = re.sub(r"\s+", " ", sql_str).strip()

        return cleaned_sql_str

    def _build_clauses(self, select_columns, conditions, partition_by, **kwargs):
        select_clause = self._build_select_clause(select_columns)
        where_clause = self._build_where_clause(conditions)
        partition_by_clause = self._build_partition_by_clause(partition_by)
        properties = self._parsed_with_properties(**kwargs)

        return select_clause, where_clause, partition_by_clause, properties

    @staticmethod
    def _build_where_clause(conditions):
        if len(conditions) > 0:
            where_clause = "where {}".format(conditions).replace('"', "'")
            return where_clause
        else:
            return ""

    @staticmethod
    def _build_select_clause(select_columns):
        select_clause = "*"
        if select_columns:
            if len(select_columns) > 0:
                select_clause = ", ".join(select_columns)

        return select_clause

    @staticmethod
    def _build_partition_by_clause(partition_by):
        if partition_by:
            return "PARTITION BY {}".format(partition_by)
        else:
            return ""
