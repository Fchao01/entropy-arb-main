#!/usr/bin/env python3
"""Read-only Variational/RH band monitor; never sends orders."""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys

import aiohttp
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from entropy_arb.config import LIGHTER_PROFILES, LighterCreds, VenueConf
from entropy_arb.book import OrderBook
from entropy_arb.recorder import MinuteRecorder
from entropy_arb.variational import (VariationalBand, VariationalReceiver,
                                     VariationalState, normalize_proxy_url)
from entropy_arb.venue_lighter import LighterVenue


async def run(args) -> None:
    proxy = normalize_proxy_url(args.proxy)
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    state = VariationalState()
    receiver = VariationalReceiver(state, ws_port=args.ws_port,
                                   rest_port=args.rest_port)
    stop, update = asyncio.Event(), asyncio.Event()
    conf = VenueConf(
        key="rh", kind="lighter", label="RH", symbol=args.symbol.upper(),
        fee_bps=0.0, cap_usd=1.0, orders_per_min=1,
        lighter_profile=LIGHTER_PROFILES["lighter-rh"],
        lighter_creds=LighterCreds(None, None, None))
    session = aiohttp.ClientSession(trust_env=True)
    rh = LighterVenue(conf, session, settle_timeout_sec=10.0)
    tasks = []
    band = VariationalBand(args.midline_bps, args.upper_bps, args.lower_bps)
    var_book = OrderBook()
    csv_path = args.csv.format(symbol=conf.symbol)
    recorder = MinuteRecorder(csv_path, var_book, rh.book,
                              staleness_sec=args.staleness)
    recorder.start()
    print(f"recording minute data to {csv_path}", flush=True)
    print("keep this running; Ctrl+C after at least 30 usable minutes will "
          "analyze and update the selected YAML / 请持续运行，至少采集 30 个有效分钟后按 Ctrl+C",
          flush=True)
    last_sample = 0.0
    last = None
    try:
        await receiver.start()
        try:
            await rh.load_market()
        except (asyncio.TimeoutError, aiohttp.ClientError) as error:
            raise RuntimeError("cannot reach Lighter RH market API; check the "
                               "network/proxy and retry") from error
        tasks = rh.start_tasks(stop, update.set, live=False)
        print(f"read-only monitor ready: SELL hurdle={band.sell_hurdle_bps:.2f} bps, "
              f"BUY hurdle={band.buy_hurdle_bps:.2f} bps", flush=True)
        while True:
            try:
                await asyncio.wait_for(update.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            update.clear()
            quote = state.quote(conf.symbol)
            rb, ra = rh.book.best_bid(), rh.book.best_ask()
            if not quote or rb is None or ra is None:
                continue
            try:
                quote_qty = float(quote["raw"].get("qty") or 0.0)
            except (TypeError, ValueError):
                quote_qty = 0.0
            if quote_qty > 0:
                var_book.apply_hl([
                    [{"px": str(quote["bid"]), "sz": str(quote_qty)}],
                    [{"px": str(quote["ask"]), "sz": str(quote_qty)}]])
                now = asyncio.get_running_loop().time()
                if now - last_sample >= 1.0:
                    recorder.sample()
                    last_sample = now
            position_row = state.positions.get(conf.symbol, {})
            try:
                position = float(position_row.get("qty") or 0.0)
            except (TypeError, ValueError):
                position = 0.0
            decision = band.decide(
                position=position, variational_bid=quote["bid"],
                variational_ask=quote["ask"], rh_bid=rb, rh_ask=ra)
            sell_edge = (quote["bid"] / ra - 1.0) * 1e4
            buy_edge = (rb / quote["ask"] - 1.0) * 1e4
            snapshot = (round(sell_edge, 2), round(buy_edge, 2), position,
                        decision[0] if decision else None)
            if snapshot != last:
                quote_usd = quote_qty * (quote["bid"] + quote["ask"]) / 2
                print(f"sell_edge={sell_edge:+.2f} buy_edge={buy_edge:+.2f} "
                      f"quote_size=${quote_usd:.2f} var_pos={position:+g} "
                      f"signal={decision or '-'}", flush=True)
                last = snapshot
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await rh.close()
        recorder.close()
        await receiver.close()
        await session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="",
                   help="read all settings from YAML and update that same YAML on exit")
    p.add_argument("--symbol")
    p.add_argument("--midline-bps", type=float)
    p.add_argument("--upper-bps", type=float)
    p.add_argument("--lower-bps", type=float)
    p.add_argument("--ws-port", type=int)
    p.add_argument("--rest-port", type=int)
    p.add_argument("--proxy",
                   help="HTTP(S) proxy, e.g. http://127.0.0.1:7897")
    p.add_argument("--csv",
                   help="minute CSV path; supports {symbol}")
    p.add_argument("--staleness", type=float)
    p.add_argument("--fees-bps", type=float)
    p.add_argument("--take-fraction", type=float)
    p.add_argument("--max-order-cap", type=float)
    p.add_argument("--threshold-buffer-bps", type=float,
                   help="extra bps added to analyzed upper/lower suggestions")
    p.add_argument("--no-analyze-on-exit", dest="analyze_on_exit",
                   action="store_false",
                   help="do not run analyze.py automatically after Ctrl+C")
    p.add_argument("--update-config", default="", metavar="YAML",
                   help="after recording, write buffered thresholds to YAML")
    p.set_defaults(analyze_on_exit=True)
    args = p.parse_args()

    if args.config:
        try:
            with open(args.config, encoding="utf-8") as fh:
                raw = yaml.safe_load(fh) or {}
        except (OSError, yaml.YAMLError) as error:
            p.error(f"cannot read config {args.config!r}: {error}")
        analysis = raw.get("analysis") or {}
        thresholds = raw.get("thresholds") or {}
        sizing = raw.get("sizing") or {}
        args.symbol = args.symbol or analysis.get("symbol")
        args.midline_bps = (args.midline_bps if args.midline_bps is not None
                            else thresholds.get("midline_bps", 2.0))
        args.upper_bps = (args.upper_bps if args.upper_bps is not None
                          else thresholds.get("upper_bps", 11.0))
        args.lower_bps = (args.lower_bps if args.lower_bps is not None
                          else thresholds.get("lower_bps", 8.5))
        args.ws_port = (args.ws_port if args.ws_port is not None
                        else analysis.get("ws_port", 8766))
        args.rest_port = (args.rest_port if args.rest_port is not None
                          else analysis.get("rest_port", 8767))
        args.proxy = args.proxy if args.proxy is not None else analysis.get("proxy", "")
        args.csv = args.csv or analysis.get("csv")
        args.staleness = (args.staleness if args.staleness is not None
                          else analysis.get("staleness_sec", 10.0))
        args.fees_bps = (args.fees_bps if args.fees_bps is not None
                         else analysis.get("fees_bps", 0.0))
        args.take_fraction = (args.take_fraction if args.take_fraction is not None
                              else sizing.get("take_fraction", 0.5))
        args.max_order_cap = (args.max_order_cap if args.max_order_cap is not None
                              else analysis.get("max_order_cap_usd", 100.0))
        args.threshold_buffer_bps = (
            args.threshold_buffer_bps if args.threshold_buffer_bps is not None
            else analysis.get("threshold_buffer_bps", 2.0))
        args.update_config = args.config
    else:
        args.midline_bps = 2.0 if args.midline_bps is None else args.midline_bps
        args.upper_bps = 11.0 if args.upper_bps is None else args.upper_bps
        args.lower_bps = 8.5 if args.lower_bps is None else args.lower_bps
        args.ws_port = 8766 if args.ws_port is None else args.ws_port
        args.rest_port = 8767 if args.rest_port is None else args.rest_port
        args.proxy = args.proxy or ""
        args.csv = args.csv or "logs/minutes_variational_rh_{symbol}.csv"
        args.staleness = 10.0 if args.staleness is None else args.staleness
        args.fees_bps = 0.0 if args.fees_bps is None else args.fees_bps
        args.take_fraction = 0.5 if args.take_fraction is None else args.take_fraction
        args.max_order_cap = 100.0 if args.max_order_cap is None else args.max_order_cap
        args.threshold_buffer_bps = (2.0 if args.threshold_buffer_bps is None
                                     else args.threshold_buffer_bps)
    if not args.symbol:
        p.error("analysis.symbol is required in the selected YAML (or use --symbol)")
    if not args.csv:
        p.error("analysis.csv is required in the selected YAML (or use --csv)")

    csv_path = args.csv.format(symbol=args.symbol.upper())
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except RuntimeError as error:
        raise SystemExit(str(error))
    if args.analyze_on_exit:
        print(f"\nrecording saved to {csv_path}; analyzing...", flush=True)
        command = [sys.executable,
                   os.path.join(os.path.dirname(__file__), "analyze.py")]
        if args.config:
            command += ["--config", args.config]
        else:
            command += ["--csv", csv_path,
                        "--fees-bps", str(args.fees_bps),
                        "--take-fraction", str(args.take_fraction),
                        "--max-order-cap", str(args.max_order_cap),
                        "--threshold-buffer-bps", str(args.threshold_buffer_bps)]
        if args.update_config and not args.config:
            command += ["--update-config", args.update_config]
        subprocess.run(command, check=False)


if __name__ == "__main__":
    main()
