"""Rich progress display for ClickHouse query runner."""

from __future__ import annotations

import collections
import time

from rich import live, progress, table


class QueryProgress:
    """Manages the Rich live display for batch and per-query progress."""

    def __init__(self, total_queries: int) -> None:
        self.total_queries = total_queries
        self.completed_count = 0
        self._recent_times: collections.deque[float] = collections.deque(
            maxlen=10
        )
        self.batch_progress = progress.Progress(
            progress.SpinnerColumn(),
            progress.TextColumn('[progress.description]{task.description}'),
            progress.BarColumn(),
            progress.TaskProgressColumn(),
            progress.MofNCompleteColumn(),
            progress.TextColumn('queries'),
            progress.TextColumn('{task.fields[rate]}'),
            progress.TimeRemainingColumn(),
        )
        self.batch_task = self.batch_progress.add_task(
            'Backfill', total=total_queries, rate=''
        )
        self.active_table = table.Table(
            title=None, box=None, show_header=True, pad_edge=False
        )
        self.active_table.add_column('Node', style='cyan', min_width=16)
        self.active_table.add_column(
            'Offset', style='white', justify='right', min_width=6
        )
        self.active_table.add_column('Progress', min_width=36)
        self.active_table.add_column(
            'Rows Read', style='green', justify='right', min_width=14
        )
        self.active_table.add_column(
            'Progress', style='yellow', justify='right', min_width=8
        )
        self.active_table.add_column(
            'Elapsed', style='blue', justify='right', min_width=8
        )
        self._active_queries: dict[str, _ActiveQuery] = {}
        self._live: live.Live | None = None

    def start(self) -> live.Live:
        """Create and return the Live context manager."""
        self._live = live.Live(self._render(), refresh_per_second=4)
        return self._live

    def mark_started(self, query_id: str, node: str, offset: int) -> None:
        """Record that a query has started executing."""
        self._active_queries[query_id] = _ActiveQuery(
            node=node, offset=offset, start_time=time.monotonic()
        )

    def update_query(
        self,
        query_id: str,
        read_rows: int,
        total_rows: int,
        elapsed: float,
        pct: float,
    ) -> None:
        """Update progress for an active query."""
        if query_id in self._active_queries:
            aq = self._active_queries[query_id]
            aq.read_rows = read_rows
            aq.total_rows = total_rows
            aq.elapsed = elapsed
            aq.pct = pct

    def mark_completed(self, query_id: str, elapsed: float) -> None:
        """Record that a query has completed."""
        self._active_queries.pop(query_id, None)
        self._recent_times.append(elapsed)
        self.completed_count += 1
        rate = self._format_rate()
        self.batch_progress.update(
            self.batch_task, completed=self.completed_count, rate=rate
        )
        self._refresh()

    def mark_skipped(self, count: int) -> None:
        """Advance progress for skipped (checkpointed) queries."""
        self.completed_count += count
        self.batch_progress.update(
            self.batch_task, completed=self.completed_count
        )

    def _format_rate(self) -> str:
        """Format the rolling average rate."""
        if not self._recent_times:
            return ''
        avg = sum(self._recent_times) / len(self._recent_times)
        return f'{avg:.1f}s/q'

    def _render(self) -> table.Table:
        """Build the composite display."""
        layout = table.Table.grid()
        layout.add_row(self.batch_progress)
        if self._active_queries:
            query_table = table.Table(
                box=None, show_header=True, pad_edge=False
            )
            query_table.add_column('Node', style='cyan', min_width=16)
            query_table.add_column(
                'Offset', style='white', justify='right', min_width=6
            )
            query_table.add_column('Progress', min_width=36)
            query_table.add_column(
                'Rows Read', style='green', justify='right', min_width=14
            )
            query_table.add_column(
                'Progress', style='yellow', justify='right', min_width=8
            )
            query_table.add_column(
                'Elapsed', style='blue', justify='right', min_width=8
            )
            for aq in self._active_queries.values():
                bar = progress.ProgressBar(
                    total=max(aq.total_rows, 1),
                    completed=aq.read_rows,
                    width=32,
                )
                rows_str = _format_rows(aq.read_rows, aq.total_rows)
                pct_str = f'{aq.pct:.1f}%' if aq.pct > 0 else ''
                elapsed_str = _format_elapsed(aq.elapsed)
                query_table.add_row(
                    aq.node,
                    str(aq.offset),
                    bar,
                    rows_str,
                    pct_str,
                    elapsed_str,
                )
            layout.add_row(query_table)
        return layout

    def _refresh(self) -> None:
        """Refresh the live display."""
        if self._live is not None:
            self._live.update(self._render())


class _ActiveQuery:
    """Track state of a currently executing query."""

    __slots__ = (
        'node',
        'offset',
        'start_time',
        'read_rows',
        'total_rows',
        'elapsed',
        'pct',
    )

    def __init__(self, node: str, offset: int, start_time: float) -> None:
        self.node = node
        self.offset = offset
        self.start_time = start_time
        self.read_rows = 0
        self.total_rows = 0
        self.elapsed = 0.0
        self.pct = 0.0


def _format_rows(read: int, total: int) -> str:
    """Format row counts for display."""
    return f'{_human_number(read)} / {_human_number(total)}'


def _human_number(n: int) -> str:
    """Format a number with K/M/B suffixes."""
    if n >= 1_000_000_000:
        return f'{n / 1_000_000_000:.1f}B'
    if n >= 1_000_000:
        return f'{n / 1_000_000:.1f}M'
    if n >= 1_000:
        return f'{n / 1_000:.1f}K'
    return str(n)


def _format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as H:MM:SS or M:SS."""
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours:
        return f'{hours}:{minutes:02d}:{secs:02d}'
    return f'{minutes}:{secs:02d}'
