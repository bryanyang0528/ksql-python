import ksql
import telnetlib


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
