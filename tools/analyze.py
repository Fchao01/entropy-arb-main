#!/usr/bin/env python3
"""Analyze recorded minute data and suggest config.yaml thresholds.

Reads the CSV written by the built-in recorder (logs/minutes.csv by default)
and prints:

  * the premium distribution (midline candidates),
  * how often each candidate upper/lower band would have fired,
  * a ready-to-paste `thresholds:` snippet.

分析机器人自动采集的分钟级盘口数据，输出溢价分布、各档阈值的触发频率，
以及可直接粘贴进 config.yaml 的 thresholds 建议值。

Usage:
    python3 tools/analyze.py                    # logs/minutes.csv
    python3 tools/analyze.py --csv path.csv --hours 24 --min-samples 10
"""
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import yaml

CANDIDATES = [1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0]


def pctl(sorted_vals: list, q: float) -> float:
    """Linear-interpolated percentile of a pre-sorted list, q in [0, 100]."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def load_rows(path: str, hours: float, min_samples: int, date: str = "") -> list:
    cutoff = time.time() - hours * 3600 if hours > 0 else 0.0
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            try:
                if date:
                    day = datetime.fromtimestamp(float(r["minute_ts"]),
                                                 tz=timezone.utc).date().isoformat()
                    if day != date:
                        continue
                if float(r["minute_ts"]) < cutoff:
                    continue
                if int(r["samples"]) < min_samples:
                    continue
                rows.append({
                    "ts": float(r["minute_ts"]),
                    "prem": float(r["premium_close_bps"]),
                    "prem_mean": float(r["premium_mean_bps"]),
                    "sell_max": float(r["sell_edge_max_bps"]),
                    "buy_max": float(r["buy_edge_max_bps"]),
                    "sell_capacity": float(r.get("sell_capacity_min_usd") or "nan"),
                    "buy_capacity": float(r.get("buy_capacity_min_usd") or "nan"),
                })
            except (KeyError, ValueError):
                continue
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="suggest thresholds from recorded "
                                            "minute data")
    p.add_argument("--config", default="",
                   help="read analysis settings from YAML and update that same file")
    p.add_argument("--csv", default=None)
    p.add_argument("--hours", type=float, default=None,
                   help="only use the last N hours (0 = all data)")
    p.add_argument("--min-samples", type=int, default=None,
                   help="skip minutes with fewer fresh samples than this")
    p.add_argument("--date", default="",
                   help="analyze this UTC date (YYYY-MM-DD), e.g. yesterday")
    p.add_argument("--update-config", default="",
                   metavar="YAML",
                   help="update thresholds in this YAML with the suggestion")
    p.add_argument("--fees-bps", type=float, default=None,
                   help="SUM of both venues' taker fees in bps (each crossing "
                        "pays both legs); recorded edges are pre-fee, so this "
                        "is subtracted before counting firings (default 0.0 — "
                        "pass ~1.0 with a tradexyz hedge)")
    p.add_argument("--take-fraction", type=float, default=None,
                   help="fraction of conservative capacity to use for sizing")
    p.add_argument("--max-order-cap", type=float, default=None,
                   help="hard ceiling for suggested max_order_notional_usd")
    p.add_argument("--threshold-buffer-bps", type=float, default=None,
                   help="safety buffer added to upper/lower suggestions")
    args = p.parse_args()

    if args.config:
        try:
            with open(args.config, encoding="utf-8") as fh:
                yaml_config = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            p.error(f"config file not found: {args.config}")
        analysis = yaml_config.get("analysis") or {}
        sizing = yaml_config.get("sizing") or {}
        args.csv = args.csv or analysis.get("csv")
        args.hours = args.hours if args.hours is not None else analysis.get("hours", 0.0)
        args.min_samples = (args.min_samples if args.min_samples is not None
                            else analysis.get("min_samples", 10))
        args.fees_bps = (args.fees_bps if args.fees_bps is not None
                         else analysis.get("fees_bps", 0.0))
        args.take_fraction = (args.take_fraction if args.take_fraction is not None
                              else sizing.get("take_fraction", 0.5))
        args.max_order_cap = (args.max_order_cap if args.max_order_cap is not None
                              else analysis.get("max_order_cap_usd", 500.0))
        args.threshold_buffer_bps = (
            args.threshold_buffer_bps if args.threshold_buffer_bps is not None
            else analysis.get("threshold_buffer_bps", 2.0))
        args.update_config = args.config
        if not args.csv:
            p.error("analysis.csv is required in the selected YAML")
    else:
        args.csv = args.csv or "logs/minutes.csv"
        args.hours = 0.0 if args.hours is None else args.hours
        args.min_samples = 10 if args.min_samples is None else args.min_samples
        args.fees_bps = 0.0 if args.fees_bps is None else args.fees_bps
        args.take_fraction = 0.5 if args.take_fraction is None else args.take_fraction
        args.max_order_cap = 500.0 if args.max_order_cap is None else args.max_order_cap
        args.threshold_buffer_bps = (2.0 if args.threshold_buffer_bps is None
                                     else args.threshold_buffer_bps)

    try:
        date = args.date
        if date == "yesterday":
            date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        if date:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError("--date must be YYYY-MM-DD or yesterday")
        rows = load_rows(args.csv, args.hours, args.min_samples, date)
    except FileNotFoundError:
        print(f"{args.csv} not found — run the bot (even --record-only) to "
              f"collect data first / 未找到数据文件，请先运行机器人采集数据",
              file=sys.stderr)
        sys.exit(1)
    if len(rows) < 30:
        print(f"only {len(rows)} usable minute(s) in {args.csv} — collect at "
              f"least a few hours before trusting the numbers / 数据太少，"
              f"建议至少采集数小时", file=sys.stderr)
        if not rows:
            sys.exit(1)
        if args.update_config:
            print("refusing to update config: fewer than 30 usable minutes",
                  file=sys.stderr)
            sys.exit(1)

    span_h = (rows[-1]["ts"] - rows[0]["ts"]) / 3600.0 + 1 / 60.0
    prem = sorted(r["prem"] for r in rows)
    mean = sum(prem) / len(prem)
    var = sum((x - mean) ** 2 for x in prem) / len(prem)
    median = pctl(prem, 50)

    print(f"\n=== {args.csv}: {len(rows)} minutes over {span_h:.1f}h ===\n")
    print("premium of Entropy over hedge, minute close (bps) / "
          "Entropy 相对对冲腿的溢价:")
    print(f"  mean {mean:+.2f}   std {math.sqrt(var):.2f}   "
          f"median {median:+.2f}")
    print(f"  p5 {pctl(prem, 5):+.2f}   p25 {pctl(prem, 25):+.2f}   "
          f"p75 {pctl(prem, 75):+.2f}   p95 {pctl(prem, 95):+.2f}")

    midline = round(median, 1) or 0.0   # normalize -0.0
    # room beyond the midline that was actually executable each minute, net
    # of taker fees (config thresholds are net-of-fee: the engine adds fees
    # on top, and recorded edges are pre-fee)
    fees = args.fees_bps
    sell_room = sorted((r["sell_max"] - midline - fees for r in rows),
                       reverse=True)
    buy_room = sorted((r["buy_max"] + midline - fees for r in rows),
                      reverse=True)

    print(f"\nwith midline_bps = {midline:+.1f} (median) and {fees:.1f} bps "
          f"round-trip taker fees, minutes each band would have fired / "
          f"各档净阈值触发的分钟数:")
    print(f"  {'band bps':>9} | {'SELL entropy':>17} | {'BUY entropy':>17}")
    print(f"  {'':>9} | {'minutes':>8} {'per day':>8} | "
          f"{'minutes':>8} {'per day':>8}")
    per_day = 24.0 / span_h if span_h > 0 else 0.0
    for t in CANDIDATES:
        s_hits = sum(1 for x in sell_room if x >= t)
        b_hits = sum(1 for x in buy_room if x >= t)
        print(f"  {t:>9.1f} | {s_hits:>8} {s_hits * per_day:>8.1f} | "
              f"{b_hits:>8} {b_hits * per_day:>8.1f}")

    # default suggestion: the band that fired in ~10% of minutes (p90 of the
    # fee-adjusted executable room), floored at 1 bps — tune from the table
    raw_upper = max(round(pctl(sorted(sell_room), 90) * 2) / 2, 1.0)
    raw_lower = max(round(pctl(sorted(buy_room), 90) * 2) / 2, 1.0)
    buffer_bps = max(args.threshold_buffer_bps, 0.0)
    sug_upper = round(raw_upper + buffer_bps, 3)
    sug_lower = round(raw_lower + buffer_bps, 3)
    sell_caps = sorted(r["sell_capacity"] for r in rows
                       if math.isfinite(r["sell_capacity"]) and r["sell_capacity"] > 0)
    buy_caps = sorted(r["buy_capacity"] for r in rows
                      if math.isfinite(r["buy_capacity"]) and r["buy_capacity"] > 0)
    sizing_text = ""
    if sell_caps and buy_caps:
        sell_size = min(pctl(sell_caps, 10) * args.take_fraction,
                        args.max_order_cap)
        buy_size = min(pctl(buy_caps, 10) * args.take_fraction,
                       args.max_order_cap)
        common_size = math.floor(min(sell_size, buy_size))
        sizing_text = f"""
conservative sizing from the 10th percentile of minute minimum top-level
capacity × take_fraction={args.take_fraction:.2f} / 根据分钟最小容量第 10 百分位估算:
  SELL primary direction: ${sell_size:.2f}
  BUY primary direction:  ${buy_size:.2f}

sizing:
  take_fraction: {args.take_fraction}
  max_order_notional_usd: {common_size}
"""
    print(f"""
raw statistical suggestion (shown unchanged; already net of fees) /
原始统计建议（保持原值显示，已扣除手续费）:

thresholds:
  midline_bps: {midline}
  upper_bps: {raw_upper}
  lower_bps: {raw_lower}

safety buffer / 安全缓冲:
  threshold_buffer_bps: {buffer_bps}

final values for YAML = raw + buffer / 最终写入 YAML = 原始值 + 缓冲:

thresholds:
  midline_bps: {midline}
  upper_bps: {sug_upper}
  lower_bps: {sug_lower}
{sizing_text}

Re-run with --hours to focus on recent regimes; premiums drift, so refresh
these numbers regularly. / 溢价中枢会漂移，请定期重新分析并更新配置。
""")

    if args.update_config:
        path = args.update_config
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        replacements = {
            "midline_bps": midline,
            "upper_bps": sug_upper,
            "lower_bps": sug_lower,
            "max_order_notional_usd": common_size if sell_caps and buy_caps else None,
        }
        for key, value in replacements.items():
            if value is None:
                continue
            # Replace only the scalar token.  Do not consume the whitespace
            # before an inline comment; YAML requires whitespace before '#'.
            pat = rf"(^\s*{re.escape(key)}\s*:\s*)[^#\s\r\n]+"
            text, n = re.subn(pat, rf"\g<1>{value}", text,
                              count=1, flags=re.MULTILINE)
            if n != 1:
                raise ValueError(f"could not find thresholds.{key} in {path}")
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
        print(f"updated thresholds in {path}")


if __name__ == "__main__":
    main()
