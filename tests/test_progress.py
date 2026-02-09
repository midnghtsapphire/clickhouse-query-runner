"""Tests for Rich progress display."""

from __future__ import annotations

import unittest
from unittest import mock

from rich import live

from clickhouse_query_runner import progress


class QueryProgressInitTests(unittest.TestCase):
    """Tests for QueryProgress initialization."""

    def test_initial_state(self) -> None:
        qp = progress.QueryProgress(10)
        self.assertEqual(qp.total_queries, 10)
        self.assertEqual(qp.completed_count, 0)
        self.assertIsNone(qp._live)

    def test_start_returns_live(self) -> None:
        qp = progress.QueryProgress(5)
        result = qp.start()
        self.assertIsInstance(result, live.Live)
        self.assertIsNotNone(qp._live)


class QueryProgressTrackingTests(unittest.TestCase):
    """Tests for query tracking."""

    def test_mark_started(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        self.assertIn('q1', qp._active_queries)
        aq = qp._active_queries['q1']
        self.assertEqual(aq.node, 'node-a')
        self.assertEqual(aq.offset, 0)

    def test_update_query(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp.update_query('q1', 500, 1000, 2.5, 50.0)
        aq = qp._active_queries['q1']
        self.assertEqual(aq.read_rows, 500)
        self.assertEqual(aq.total_rows, 1000)
        self.assertEqual(aq.elapsed, 2.5)
        self.assertEqual(aq.pct, 50.0)

    def test_update_unknown_query(self) -> None:
        qp = progress.QueryProgress(5)
        qp.update_query('unknown', 500, 1000, 2.5, 50.0)
        self.assertNotIn('unknown', qp._active_queries)

    def test_mark_completed(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp.mark_completed('q1', 1.5)
        self.assertNotIn('q1', qp._active_queries)
        self.assertEqual(qp.completed_count, 1)

    def test_mark_completed_unknown(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_completed('unknown', 1.0)
        self.assertEqual(qp.completed_count, 1)

    def test_mark_skipped(self) -> None:
        qp = progress.QueryProgress(10)
        qp.mark_skipped(3)
        self.assertEqual(qp.completed_count, 3)

    def test_mark_skipped_then_completed(self) -> None:
        qp = progress.QueryProgress(10)
        qp.mark_skipped(3)
        qp.mark_completed('q1', 1.0)
        self.assertEqual(qp.completed_count, 4)


class FormatRateTests(unittest.TestCase):
    """Tests for _format_rate."""

    def test_no_times(self) -> None:
        qp = progress.QueryProgress(5)
        self.assertEqual(qp._format_rate(), '')

    def test_single_time(self) -> None:
        qp = progress.QueryProgress(5)
        qp._recent_times.append(2.0)
        self.assertEqual(qp._format_rate(), '2.0s/q')

    def test_multiple_times_averaged(self) -> None:
        qp = progress.QueryProgress(5)
        qp._recent_times.append(1.0)
        qp._recent_times.append(3.0)
        self.assertEqual(qp._format_rate(), '2.0s/q')


class RenderTests(unittest.TestCase):
    """Tests for _render."""

    def test_render_without_active_queries(self) -> None:
        qp = progress.QueryProgress(5)
        layout = qp._render()
        self.assertIsNotNone(layout)

    def test_render_with_active_queries(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        qp._active_queries['q1'].read_rows = 500
        qp._active_queries['q1'].total_rows = 1000
        qp._active_queries['q1'].elapsed = 2.5
        qp._active_queries['q1'].pct = 50.0
        layout = qp._render()
        self.assertIsNotNone(layout)

    def test_render_with_zero_pct(self) -> None:
        qp = progress.QueryProgress(5)
        qp.mark_started('q1', 'node-a', 0)
        layout = qp._render()
        self.assertIsNotNone(layout)


class RefreshTests(unittest.TestCase):
    """Tests for _refresh."""

    def test_refresh_without_live(self) -> None:
        qp = progress.QueryProgress(5)
        qp._refresh()  # Should not raise

    def test_refresh_with_live(self) -> None:
        qp = progress.QueryProgress(5)
        qp._live = mock.MagicMock()
        qp._refresh()
        qp._live.update.assert_called_once()


class HumanNumberTests(unittest.TestCase):
    """Tests for _human_number."""

    def test_small(self) -> None:
        self.assertEqual(progress._human_number(0), '0')
        self.assertEqual(progress._human_number(999), '999')

    def test_thousands(self) -> None:
        self.assertEqual(progress._human_number(1000), '1.0K')
        self.assertEqual(progress._human_number(1500), '1.5K')
        self.assertEqual(progress._human_number(999_999), '1000.0K')

    def test_millions(self) -> None:
        self.assertEqual(progress._human_number(1_000_000), '1.0M')
        self.assertEqual(progress._human_number(2_500_000), '2.5M')

    def test_billions(self) -> None:
        self.assertEqual(progress._human_number(1_000_000_000), '1.0B')
        self.assertEqual(progress._human_number(3_500_000_000), '3.5B')


class FormatRowsTests(unittest.TestCase):
    """Tests for _format_rows."""

    def test_format_small(self) -> None:
        self.assertEqual(progress._format_rows(500, 999), '500 / 999')

    def test_format_large(self) -> None:
        self.assertEqual(
            progress._format_rows(1_500_000, 4_800_000), '1.5M / 4.8M'
        )

    def test_format_mixed(self) -> None:
        self.assertEqual(progress._format_rows(500, 1_000_000), '500 / 1.0M')


class FormatElapsedTests(unittest.TestCase):
    """Tests for _format_elapsed."""

    def test_seconds_only(self) -> None:
        self.assertEqual(progress._format_elapsed(5.0), '0:05')

    def test_minutes_and_seconds(self) -> None:
        self.assertEqual(progress._format_elapsed(65.0), '1:05')

    def test_hours(self) -> None:
        self.assertEqual(progress._format_elapsed(3661.0), '1:01:01')

    def test_zero(self) -> None:
        self.assertEqual(progress._format_elapsed(0.0), '0:00')


class ActiveQueryTests(unittest.TestCase):
    """Tests for _ActiveQuery."""

    def test_defaults(self) -> None:
        aq = progress._ActiveQuery('node1', 5, 100.0)
        self.assertEqual(aq.node, 'node1')
        self.assertEqual(aq.offset, 5)
        self.assertEqual(aq.start_time, 100.0)
        self.assertEqual(aq.read_rows, 0)
        self.assertEqual(aq.total_rows, 0)
        self.assertEqual(aq.elapsed, 0.0)
        self.assertEqual(aq.pct, 0.0)
