import ksql
import telnetlib
import json
import re


def check_kafka_available(bootstrap_servers):
    host, port = bootstrap_servers.split(":")
    try:
        telnetlib.Telnet(host, port)
        return True
    except Exception:
        return False


def get_all_streams(api_client, prefix=None):
    all_streams = api_client.ksql("""SHOW STREAMS;""")
    filtered_streams = []
    for stream in all_streams[0]["streams"]:
        if stream["type"] != "STREAM":
            continue
        if prefix and not stream["name"].startswith(prefix.upper()):
            continue
        filtered_streams.append(stream["name"])
    return filtered_streams


def get_stream_info(api_client, stream_name):
    try:
        r = api_client.ksql("""DESCRIBE EXTENDED {}""".format(stream_name))
    except ksql.errors.KSQLError as e:
        if e.error_code == 40001:
            return None
        else:
            raise
    stream_info = r[0]["sourceDescription"]
    return stream_info


def drop_all_streams(api_client, prefix=None):
    filtered_streams = get_all_streams(api_client, prefix=prefix)
    for stream in filtered_streams:
        drop_stream(api_client, stream)


def drop_stream(api_client, stream_name):
    read_queries, write_queries = get_dependent_queries(api_client, stream_name)
    dependent_queries = read_queries + write_queries
    for query in dependent_queries:
        api_client.ksql("""TERMINATE {};""".format(query))
    api_client.ksql(
        """DROP
    STREAM IF EXISTS
    {};""".format(
            stream_name
        )
    )


def get_dependent_queries(api_client, stream_name):
    stream_info = get_stream_info(api_client, stream_name)
    read_queries = []
    write_queries = []
    if stream_info and stream_info["readQueries"]:
        read_queries = [query["id"] for query in stream_info["readQueries"]]
    if stream_info and stream_info["writeQueries"]:
        write_queries = [query["id"] for query in stream_info["writeQueries"]]

    return read_queries, write_queries


def parse_columns(columns_str):
    regex = r"(?<!\<)`(?P<name>[A-Z_]+)` (?P<type>[A-z]+)[\<, \"](?!\>)"
    result = []

    matches = re.finditer(regex, columns_str)
    for matchNum, match in enumerate(matches, start=1):
        result.append({"name": match.group("name"), "type": match.group("type")})

    return result


def process_row(row, column_names):
    row = row.replace(",\n", "").replace("]\n", "")
    row_obj = json.loads(row)
    if 'finalMessage' in row_obj:
        return None
    column_values = row_obj["row"]["columns"]
    index = 0
    result = {}
    for column in column_values:
        result[column_names[index]["name"]] = column
        index += 1

    return result


def process_query_result(results, return_objects=None):
    if return_objects is None:
        yield from results

    # parse rows into objects
    header = next(results)
    columns = parse_columns(header)

    for result in results:
        row_obj = process_row(result, columns)
        if row_obj is None:
            return
        yield row_obj

