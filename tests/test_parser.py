"""Tests for SQL file parsing."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest

from clickhouse_query_runner import parser


class ParseSqlFileTests(unittest.TestCase):
    """Tests for parse_sql_file."""

    def _write_temp(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix='.sql')
        os.write(fd, content.encode())
        os.close(fd)
        self.addCleanup(os.unlink, path)
        return path

    def _hash(self, sql: str) -> str:
        return hashlib.sha256(sql.strip().encode()).hexdigest()

    def test_single_statement(self) -> None:
        path = self._write_temp('SELECT 1;')
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], self._hash('SELECT 1'))
        self.assertEqual(result[0][1], 'SELECT 1')

    def test_multiple_statements(self) -> None:
        path = self._write_temp('SELECT 1;\nSELECT 2;\nSELECT 3;')
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][1], 'SELECT 1')
        self.assertEqual(result[1][1], 'SELECT 2')
        self.assertEqual(result[2][1], 'SELECT 3')

    def test_empty_file(self) -> None:
        path = self._write_temp('')
        result = parser.parse_sql_file(path)
        self.assertEqual(result, [])

    def test_whitespace_only(self) -> None:
        path = self._write_temp('   \n\n  \t  ')
        result = parser.parse_sql_file(path)
        self.assertEqual(result, [])

    def test_empty_statements_skipped(self) -> None:
        path = self._write_temp('SELECT 1;;;\nSELECT 2;')
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 2)

    def test_no_trailing_semicolon(self) -> None:
        path = self._write_temp('SELECT 1;\nSELECT 2')
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1][1], 'SELECT 2')

    def test_semicolon_in_single_quoted_string(self) -> None:
        sql = "INSERT INTO t VALUES ('a;b');"
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn("'a;b'", result[0][1])

    def test_semicolon_in_double_quoted_identifier(self) -> None:
        sql = 'SELECT "col;name" FROM t;'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn('"col;name"', result[0][1])

    def test_single_line_comment(self) -> None:
        sql = '-- comment\nSELECT 1;'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn('-- comment', result[0][1])

    def test_single_line_comment_at_eof(self) -> None:
        sql = 'SELECT 1;\n-- trailing comment'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 2)

    def test_block_comment(self) -> None:
        sql = '/* block */\nSELECT 1;'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)
        self.assertIn('/* block */', result[0][1])

    def test_block_comment_with_semicolon(self) -> None:
        sql = '/* a;b */\nSELECT 1;'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)

    def test_unclosed_block_comment(self) -> None:
        sql = 'SELECT 1; /* unclosed'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 2)

    def test_escaped_quote_backslash(self) -> None:
        sql = "SELECT 'it\\'s';"
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)

    def test_escaped_quote_doubled(self) -> None:
        sql = "SELECT 'it''s';"
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)

    def test_hash_is_deterministic(self) -> None:
        path = self._write_temp('SELECT 1;')
        r1 = parser.parse_sql_file(path)
        r2 = parser.parse_sql_file(path)
        self.assertEqual(r1[0][0], r2[0][0])

    def test_hash_strips_whitespace(self) -> None:
        path1 = self._write_temp('  SELECT 1  ;')
        path2 = self._write_temp('SELECT 1;')
        r1 = parser.parse_sql_file(path1)
        r2 = parser.parse_sql_file(path2)
        self.assertEqual(r1[0][0], r2[0][0])

    def test_unclosed_single_quote(self) -> None:
        sql = "SELECT 'unterminated"
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)

    def test_unclosed_double_quote(self) -> None:
        sql = 'SELECT "unterminated'
        path = self._write_temp(sql)
        result = parser.parse_sql_file(path)
        self.assertEqual(len(result), 1)


class SplitStatementsTests(unittest.TestCase):
    """Tests for _split_statements."""

    def test_empty_string(self) -> None:
        self.assertEqual(parser._split_statements(''), [])

    def test_single_statement_no_semicolon(self) -> None:
        result = parser._split_statements('SELECT 1')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 'SELECT 1')

    def test_preserves_statement_order(self) -> None:
        content = 'A;\nB;\nC;'
        result = parser._split_statements(content)
        texts = [s.strip() for s in result]
        self.assertEqual(texts, ['A', 'B', 'C'])

    def test_dash_not_comment(self) -> None:
        """A single dash is not a comment."""
        result = parser._split_statements('SELECT 1 - 2;')
        self.assertEqual(len(result), 1)


class FindClosingQuoteTests(unittest.TestCase):
    """Tests for _find_closing_quote."""

    def test_simple(self) -> None:
        self.assertEqual(parser._find_closing_quote("'abc'", 0, "'"), 4)

    def test_escaped_backslash(self) -> None:
        self.assertEqual(parser._find_closing_quote("'a\\'b'", 0, "'"), 5)

    def test_doubled_quote(self) -> None:
        self.assertEqual(parser._find_closing_quote("'a''b'", 0, "'"), 5)

    def test_no_closing(self) -> None:
        result = parser._find_closing_quote("'abc", 0, "'")
        self.assertEqual(result, 3)
