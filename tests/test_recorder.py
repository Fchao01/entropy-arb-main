"""Minute recorder: aggregation, rollover, CSV output.

Run:  python3 -m pytest tests/  (or  python3 tests/test_recorder.py)
"""
import csv
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.recorder import HEADER, MinuteRecorder  # noqa: E402


def set_book(book, bid, ask):
    book.apply_hl([[{"px": str(bid), "sz": "10"}],
                   [{"px": str(ask), "sz": "10"}]])


def test_minute_aggregation_and_rollover():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)

    t0 = 1_700_000_000.0            # 20s into a minute (boundary at ...020)
    # minute 1: entropy 10 bps rich, then 20 bps rich
    set_book(e_book, 100.09, 100.11)   # mid 100.10
    set_book(h_book, 99.99, 100.01)    # mid 100.00
    rec.sample(t0)
    set_book(e_book, 100.19, 100.21)   # mid 100.20
    rec.sample(t0 + 10)
    # next minute: back to 10 bps rich -> flushes minute 1
    set_book(e_book, 100.09, 100.11)
    rec.sample(t0 + 45)
    rec.close()                        # flushes the partial minute 2

    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert [*rows[0]] == HEADER
    assert len(rows) == 2
    m1, m2 = rows
    assert int(m1["samples"]) == 2 and int(m2["samples"]) == 1
    assert abs(float(m1["premium_open_bps"]) - 10.0) < 0.2
    assert abs(float(m1["premium_high_bps"]) - 20.0) < 0.2
    assert abs(float(m1["premium_close_bps"]) - 20.0) < 0.2
    assert abs(float(m1["premium_mean_bps"]) - 15.0) < 0.2
    # executable edges: sell = bid_e/ask_h - 1, buy = bid_h/ask_e - 1
    assert abs(float(m2["sell_edge_max_bps"])
               - ((100.09 / 100.01 - 1) * 1e4)) < 0.05
    assert abs(float(m2["buy_edge_max_bps"])
               - ((99.99 / 100.11 - 1) * 1e4)) < 0.05
    # closes carry the last books
    assert float(m2["entropy_bid"]) == 100.09
    assert float(m2["hedge_ask"]) == 100.01
    assert float(m2["sell_capacity_min_usd"]) > 0
    assert float(m2["buy_capacity_min_usd"]) > 0


def test_stale_books_are_skipped():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)
    rec.sample(1_700_000_000.0)        # both books empty -> nothing recorded
    set_book(e_book, 100.0, 100.02)    # only one side fresh
    rec.sample(1_700_000_001.0)
    rec.close()
    assert rec.rows_written == 0
    assert not os.path.exists(path)    # no row, no file


def test_append_keeps_single_header():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)
    for start in (1_700_000_000.0, 1_700_000_060.0):
        rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)
        rec.sample(start)
        rec.close()
    with open(path) as fh:
        lines = fh.read().strip().splitlines()
    assert len(lines) == 3             # one header + two rows
    assert lines[0].startswith("minute_ts,")


def test_four_hour_period_paths_do_not_accumulate_suffixes():
    e_book, h_book = OrderBook(), OrderBook()
    rec = MinuteRecorder(os.path.join(tempfile.mkdtemp(), "minutes_LIT.csv"),
                         e_book, h_book, staleness_sec=1e9)
    rec.enable_period_files(4)
    ts1 = time.mktime((2026, 9, 11, 8, 30, 0, 0, 0, -1))
    ts2 = time.mktime((2026, 9, 11, 12, 30, 0, 0, 0, -1))
    p1, label1 = rec._period_path(ts1)
    rec.path = p1
    p2, label2 = rec._period_path(ts2)
    assert p1.endswith("minutes_LIT_20260911_0800-1200.csv")
    assert p2.endswith("minutes_LIT_20260911_1200-1600.csv")
    assert "08:00" in label1 and "12:00" in label1
    assert "12:00" in label2 and "16:00" in label2


def test_daily_period_is_anchored_at_eight_am():
    e_book, h_book = OrderBook(), OrderBook()
    rec = MinuteRecorder(os.path.join(tempfile.mkdtemp(), "minutes_SNDK.csv"),
                         e_book, h_book, staleness_sec=1e9)
    rec.enable_period_files(24, anchor_hour=8)
    before = time.mktime((2026, 9, 11, 7, 59, 0, 0, 0, -1))
    after = time.mktime((2026, 9, 11, 8, 1, 0, 0, 0, -1))
    p1, label1 = rec._period_path(before)
    p2, label2 = rec._period_path(after)
    assert p1.endswith("minutes_SNDK_20260910_0800-0800.csv")
    assert "2026-09-10 08:00" in label1 and "2026-09-11 08:00" in label1
    assert p2.endswith("minutes_SNDK_20260911_0800-0800.csv")
    assert "2026-09-12 08:00" in label2


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
