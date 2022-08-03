import unittest

import ksql.utils


class TestKSQLUtils(unittest.TestCase):
    """Test case for the client methods."""

    def test_process_header(self):
        header_str = '[{"header":{"queryId":"query_1643298761990","schema":"`COMPANY_UID` STRING KEY, `USER_UID` STRING KEY, `USER_STATUS_ID` BIGINT KEY, `BONUS_PCT` STRING"}},\n'
        actual_columns = ksql.utils.parse_columns(header_str)
        expected_columns = [
            {"name": "COMPANY_UID", "type": "STRING"},
            {"name": "USER_UID", "type": "STRING"},
            {"name": "USER_STATUS_ID", "type": "BIGINT"},
            {"name": "BONUS_PCT", "type": "STRING"},
        ]
        self.assertEqual(actual_columns, expected_columns)

    def test_process_row_with_no_dangling_closing_bracket(self):
        columns = [
            {"name": "COMPANY_UID", "type": "STRING"},
            {"name": "USER_UID", "type": "STRING"},
            {"name": "USER_STATUS_ID", "type": "BIGINT"},
            {"name": "BONUS_PCT", "type": "STRING"},
        ]
        row = '{"row":{"columns":["f08c77db7","fcafb7c23",11508,"1.10976000000000000000"]}},\n'

        actual = ksql.utils.process_row(row, columns)
        expected = {
            "BONUS_PCT": "1.10976000000000000000",
            "COMPANY_UID": "f08c77db7",
            "USER_UID": "fcafb7c23",
            "USER_STATUS_ID": 11508,
        }
        self.assertEqual(actual, expected)

    def test_process_row_with_dangling_closing_bracket(self):
        columns = [
            {"name": "COMPANY_UID", "type": "STRING"},
            {"name": "USER_UID", "type": "STRING"},
            {"name": "USER_STATUS_ID", "type": "BIGINT"},
            {"name": "BONUS_PCT", "type": "STRING"},
        ]
        row = '{"row":{"columns":["f08c77db7","fdcacbca1",13120,"1.09760000000000000000"]}}]'

        actual = ksql.utils.process_row(row, columns)
        expected = {
            "BONUS_PCT": "1.09760000000000000000",
            "COMPANY_UID": "f08c77db7",
            "USER_UID": "fdcacbca1",
            "USER_STATUS_ID": 13120,
        }
        self.assertEqual(actual, expected)


    def test_process_query_results(self):
        results = (
            r
            for r in [
                '[{"header":{"queryId":"query_1643298761990","schema":"`COMPANY_UID` STRING KEY, `USER_UID` STRING KEY, `USER_STATUS_ID` BIGINT KEY, `BONUS_PCT` STRING"}},\n',
                '{"row":{"columns":["f08c77db7","fcafb7c23",11508,"1.10976000000000000000"]}},\n',
                '{"row":{"columns":["f08c77db7","fdcacbca1",13120,"1.09760000000000000000"]}}]',
            ]
        )

        actual = list(ksql.utils.process_query_result(results, return_objects=True))
        expected = [
            {
                "BONUS_PCT": "1.10976000000000000000",
                "COMPANY_UID": "f08c77db7",
                "USER_STATUS_ID": 11508,
                "USER_UID": "fcafb7c23",
            },
            {
                "BONUS_PCT": "1.09760000000000000000",
                "COMPANY_UID": "f08c77db7",
                "USER_STATUS_ID": 13120,
                "USER_UID": "fdcacbca1",
            },
        ]
        self.assertEqual(actual, expected)
