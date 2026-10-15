#!/usr/bin/env python3
"""Compare session-list SQL with deterministic, interleaved sessions.

Run: python3 benches/session_list.py

Measures SQLite query work in memory, excluding HTTP, JSON decoding, file I/O,
and production end-to-end latency. Each query returns 51 metadata rows.
"""

import argparse
import sqlite3
import statistics
import time
from pathlib import Path

PAGE_SIZE = 51


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=positive_int, default=200_000)
    parser.add_argument("--sessions", type=positive_int, default=1_000)
    parser.add_argument("--iterations", type=positive_int, default=30)
    args = parser.parse_args()
    if args.rows < PAGE_SIZE * args.sessions:
        parser.error(f"--rows must be at least {PAGE_SIZE} * --sessions")

    schema = Path(__file__).resolve().parents[1] / "src/session_log/schema.sql"
    connection = sqlite3.connect(":memory:")
    connection.executescript(schema.read_text())
    connection.executemany(
        "INSERT INTO sessions VALUES (?, ?)",
        ((f"s{i}", "bench") for i in range(args.sessions)),
    )
    connection.executemany(
        "INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            (
                f"r{i:09}",
                f"s{i % args.sessions}",
                i,
                None,
                None,
                "success",
                f'{{"id":"r{i:09}"}}',
            )
            for i in range(args.rows)
        ),
    )
    connection.commit()
    session = f"s{args.sessions * 9 // 10}"
    scenarios = [
        (
            "original optional-session OR",
            "SELECT data FROM requests WHERE started_at_ms>=?1 "
            "AND (?2 IS NULL OR session_id=?2) "
            "AND (started_at_ms<?3 OR (started_at_ms=?3 AND id<?4)) "
            "ORDER BY started_at_ms DESC,id DESC LIMIT ?5",
            (0, session, 2**63 - 1, "", PAGE_SIZE),
        ),
        (
            "optimized session equality",
            "SELECT data FROM requests WHERE session_id=?5 "
            "AND started_at_ms>=?1 "
            "AND (started_at_ms<?2 OR (started_at_ms=?2 AND id<?3)) "
            "ORDER BY started_at_ms DESC,id DESC LIMIT ?4",
            (0, 2**63 - 1, "", PAGE_SIZE, session),
        ),
    ]
    print(
        f"SQLite {sqlite3.sqlite_version}; {args.rows:,} rows / {args.sessions:,} sessions"
    )
    print(f"{args.iterations} timed iterations per query; {PAGE_SIZE} rows per result")
    results = []
    medians = []
    for name, query, parameters in scenarios:
        print(f"\n{name}")
        for plan in connection.execute("EXPLAIN QUERY PLAN " + query, parameters):
            print(f"  {plan[-1]}")
        # Warm each statement before timing and keep records for equivalence checks.
        results.append(list(connection.execute(query, parameters)))
        samples = []
        for _ in range(args.iterations):
            started = time.perf_counter_ns()
            list(connection.execute(query, parameters))
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
        medians.append(statistics.median(samples))
        print(f"  median {medians[-1]:.3f} ms; mean {statistics.mean(samples):.3f} ms")
    connection.close()
    if results[0] != results[1] or len(results[0]) != PAGE_SIZE:
        raise RuntimeError("query results differ or contain fewer than 51 rows")
    print(f"\nMedian SQL speedup: {medians[0] / medians[1]:.1f}x")


if __name__ == "__main__":
    main()
