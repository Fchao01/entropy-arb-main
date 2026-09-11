#!/usr/bin/env python3
"""Threshold-driven Variational browser execution with Lighter RH hedging.

Dry-run is the default.  ``--live`` requires both the CLI acknowledgement and
the separate live-arm button in the Chrome extension.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import sys
import time
from collections import deque
from types import SimpleNamespace
from typing import Optional

import aiohttp
import yaml
from aiohttp import web
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from entropy_arb.config import LIGHTER_PROFILES, LighterCreds, VenueConf
from entropy_arb.book import OrderBook
from entropy_arb.dashboard import BufferLogHandler
from entropy_arb.recorder import MinuteRecorder
from entropy_arb.variational_dashboard import VariationalDashboard
from entropy_arb.variational import (VariationalBand, VariationalCommandServer,
                                     VariationalReceiver, VariationalState,
                                     canonical_symbol,
                                     normalize_proxy_url)
from entropy_arb.venue_lighter import LighterVenue


class RhSignerProxyBridge:
    """Loopback reverse proxy for the SDK's native signer HTTP client."""

    def __init__(self, session: aiohttp.ClientSession, upstream: str) -> None:
        self.session, self.upstream = session, upstream.rstrip("/")
        self.runner = None
        self.url = ""

    async def start(self) -> None:
        async def forward(request: web.Request) -> web.Response:
            headers = {k: v for k, v in request.headers.items()
                       if k.lower() not in {"host", "content-length", "connection",
                                            "transfer-encoding"}}
            body = await request.read()
            async with self.session.request(
                    request.method, self.upstream + request.raw_path,
                    headers=headers, data=body, allow_redirects=False) as response:
                response_body = await response.read()
                response_headers = {k: v for k, v in response.headers.items()
                                    if k.lower() not in {"content-length", "connection",
                                                         "transfer-encoding",
                                                         "content-encoding"}}
                return web.Response(status=response.status, body=response_body,
                                    headers=response_headers)

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", forward)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
        self.runner = None


def _credentials() -> LighterCreds:
    account = os.getenv("LIGHTER_RH_ACCOUNT_INDEX") or os.getenv("LIGHTER_ACCOUNT_INDEX")
    key = os.getenv("LIGHTER_RH_API_KEY_INDEX") or os.getenv("LIGHTER_API_KEY_INDEX")
    private = (os.getenv("LIGHTER_RH_API_PRIVATE_KEY")
               or os.getenv("LIGHTER_API_PRIVATE_KEY"))
    return LighterCreds(int(account) if account else None,
                        int(key) if key else None, private)


def _position(state: VariationalState, symbol: str) -> float:
    try:
        return float(state.positions.get(symbol, {}).get("qty") or 0.0)
    except (TypeError, ValueError):
        return 0.0


async def _confirmed_fill(state: VariationalState, symbol: str, side: str,
                          timeout: float) -> tuple[float, str, Optional[float]]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise RuntimeError("Variational fill confirmation timed out; inspect the page and hedge manually")
        event = await asyncio.wait_for(state.trade_events.get(), remaining)
        instrument = event.get("instrument") or {}
        if canonical_symbol(instrument.get("underlying", "")) != canonical_symbol(symbol):
            continue
        if str(event.get("side", "")).upper() != side:
            continue
        if str(event.get("status", "")).lower() != "confirmed":
            continue
        try:
            qty = float(event.get("qty"))
        except (TypeError, ValueError):
            continue
        if qty > 0:
            price = None
            for key in ("avg_price", "average_price", "fill_price", "price", "execution_price"):
                try:
                    value = float(event.get(key))
                    if value > 0:
                        price = value
                        break
                except (TypeError, ValueError):
                    pass
            return qty, str(event.get("id", "")), price


async def _wait_sized_quote(state: VariationalState, symbol: str, qty: float,
                            timeout: float = 5.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        quote = state.quote(symbol)
        if quote:
            try:
                quoted_qty = float(quote["raw"].get("qty") or 0)
            except (TypeError, ValueError):
                quoted_qty = 0.0
            if quoted_qty > 0 and abs(quoted_qty - qty) <= max(1e-6, qty * 0.02):
                return quote
        await asyncio.sleep(0.1)
    raise RuntimeError("Variational did not return a quote for the prepared quantity")


async def run(args) -> None:
    with open(args.config, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    analysis, thresholds = raw.get("analysis") or {}, raw.get("thresholds") or {}
    sizing, execution = raw.get("sizing") or {}, raw.get("execution") or {}
    recorder_cfg = raw.get("recorder") or {}
    auto = raw.get("variational_auto") or {}
    symbol = str(analysis.get("symbol") or "").upper()
    if not symbol:
        raise RuntimeError("analysis.symbol is missing from YAML")
    notional = float(sizing.get("max_order_notional_usd", 0))
    minimum = float(sizing.get("min_order_notional_usd", 10))
    persist = float(auto.get("signal_persist_sec", 1.0))
    cooldown = float(auto.get("cooldown_sec", 10.0))
    fill_timeout = float(auto.get("fill_timeout_sec", 20.0))
    max_trades = int(auto.get("max_trades_per_session", 10))
    command_port = int(auto.get("command_port", 8768))
    staleness = float(analysis.get("staleness_sec", 10.0))
    recorder_enabled = bool(recorder_cfg.get("enabled", True))
    # The Variational analysis path is authoritative here; this keeps the
    # live collector feeding the same file consumed by tools/analyze.py.
    recorder_path = str(analysis.get("csv") or
                        recorder_cfg.get("csv") or
                        "logs/minutes_variational_rh_{symbol}.csv")
    recorder_path = recorder_path.format(symbol=symbol)
    proxy = normalize_proxy_url(str(analysis.get("proxy") or ""))
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    load_dotenv(args.env_file)
    creds = _credentials()
    if args.live and not creds.complete:
        raise RuntimeError("incomplete Lighter RH credentials")
    if args.live and (notional < minimum or notional <= 0):
        raise RuntimeError(f"refusing live mode: max order ${notional:g} is below configured minimum ${minimum:g}")

    conf = VenueConf("rh", "lighter", "RH", symbol, 0.0,
                     float((raw.get("hedge") or {}).get("max_position_usd", 1000)),
                     int((raw.get("hedge") or {}).get("max_orders_per_min", 30)),
                     lighter_profile=LIGHTER_PROFILES["lighter-rh"],
                     lighter_creds=creds)
    state = VariationalState()
    var_book = OrderBook()
    receiver = VariationalReceiver(state, ws_port=int(analysis.get("ws_port", 8766)),
                                   rest_port=int(analysis.get("rest_port", 8767)))
    commands = VariationalCommandServer(port=command_port)
    session = aiohttp.ClientSession(trust_env=True)
    bridge = RhSignerProxyBridge(session, LIGHTER_PROFILES["lighter-rh"].api_url)
    rh = LighterVenue(conf, session, float(execution.get("settle_timeout_sec", 10)))
    recorder = (MinuteRecorder(recorder_path, var_book, rh.book, staleness)
                if recorder_enabled else None)
    view = SimpleNamespace(
        stop=asyncio.Event(), start_ts=time.time(), live=args.live, symbol=symbol,
        notional=notional, staleness=staleness, state=state, var_book=var_book,
        rh=rh, commands=commands, recorder=recorder, band=None, ready=False,
        missing="启动中", var_pos=0.0, rh_pos=0.0, sell_edge=None,
        buy_edge=None, signal="-", next_condition="-", total_profit=0.0,
        total_volume=0.0, last_profit=None, last_volume=None, trades=0,
        recent_trades=deque(maxlen=10))
    stop, update = asyncio.Event(), asyncio.Event()
    tasks = []
    try:
        await receiver.start()
        await commands.start()
        await rh.load_market()
        if args.live:
            if notional < rh.min_quote:
                raise RuntimeError(f"refusing live mode: RH minimum is ${rh.min_quote:g}, configured order is ${notional:g}")
            await bridge.start()
            rh.signer_api_url = bridge.url
            rh.init_signer(validate=False)
            signer_error = await asyncio.to_thread(rh.signer.check_client)
            if signer_error is not None:
                raise RuntimeError(f"[RH] API key check failed: {signer_error}")
            logging.info("[RH] signer ready (account %d)", creds.account_index)
        tasks = rh.start_tasks(stop, update.set, live=args.live)
        if recorder is not None:
            recorder.start()
            tasks.append(asyncio.create_task(recorder.run(stop), name="recorder"))
        band = VariationalBand(float(thresholds["midline_bps"]),
                              float(thresholds["upper_bps"]),
                              float(thresholds["lower_bps"]))
        view.band = band
        if getattr(args, "dashboard", False):
            tasks.append(asyncio.create_task(
                VariationalDashboard(view, args.log_buffer, args.log_file).run(),
                name="dashboard"))
        mode = "LIVE" if args.live else "DRY-RUN"
        logging.warning("%s auto strategy: %s, notional=$%.2f, "
                        "OPEN_SHORT sell_edge>=%.2f, OPEN_LONG buy_edge>=%.2f, "
                        "CLOSE_LONG sell_edge>=%.2f, CLOSE_SHORT buy_edge>=%.2f",
                        mode, symbol, notional, band.sell_hurdle_bps,
                        band.buy_hurdle_bps, band.long_exit_bps,
                        band.short_exit_bps)
        logging.warning("start/restart the Chrome extension; live mode also requires clicking 实盘未武装")
        armed_since = None
        last_action = None
        last_trade = 0.0
        last_status = 0.0
        trades = 0
        total_profit = 0.0
        total_volume = 0.0
        last_cycle_profit = None
        last_cycle_volume = None
        rh_pos = 0.0
        last_account_poll = 0.0
        last_var_quote_received = None
        while trades < max_trades:
            await asyncio.sleep(0.25)
            quote = state.quote(symbol)
            if quote and id(quote) != last_var_quote_received:
                try:
                    quote_qty = float(quote["raw"].get("qty") or 0.0)
                except (KeyError, TypeError, ValueError):
                    quote_qty = 0.0
                if quote_qty <= 0:
                    quote_qty = notional / ((quote["bid"] + quote["ask"]) / 2.0)
                var_book.apply_hl([
                    [{"px": quote["bid"], "sz": quote_qty}],
                    [{"px": quote["ask"], "sz": quote_qty}],
                ])
                last_var_quote_received = id(quote)
            rb, ra = rh.book.best_bid(), rh.book.best_ask()
            ready = (quote and rb and ra and commands.connected.is_set()
                     and "/portfolio" in state.connected_channels
                     and rh.book.is_fresh(staleness))
            if args.live:
                ready = ready and rh.ready_to_trade()
            if not ready:
                view.ready = False
                view.var_pos, view.rh_pos = _position(state, symbol), rh_pos
                armed_since = None
                if time.monotonic() - last_status >= 5:
                    missing = []
                    if not quote: missing.append("Variational quote")
                    if not rb or not ra: missing.append("RH book")
                    if not commands.connected.is_set(): missing.append("extension command channel")
                    if "/portfolio" not in state.connected_channels: missing.append("Variational portfolio")
                    if args.live and not rh.ready_to_trade(): missing.append("RH account stream")
                    view.missing = ", ".join(missing) or "行情刷新中"
                    logging.info("waiting for: %s; Variational symbols seen=%s; "
                                 "var_pos=%+.8g RH_pos=%+.8g last_profit=%s "
                                 "total_profit=%+.4f total_volume=$%.2f",
                                 ", ".join(missing) or "fresh feeds",
                                 sorted(state.quotes) or "none", _position(state, symbol),
                                 rh_pos,
                                 f"${last_cycle_profit:+.4f}" if last_cycle_profit is not None else "—",
                                 total_profit, total_volume)
                    last_status = time.monotonic()
                continue
            var_pos = _position(state, symbol)
            if args.live and time.monotonic() - last_account_poll >= 10:
                try:
                    rh_pos = await rh.fetch_position()
                except Exception as error:
                    logging.debug("RH position refresh failed: %s", error)
                last_account_poll = time.monotonic()
            action = band.decide(position=var_pos, variational_bid=quote["bid"],
                                 variational_ask=quote["ask"], rh_bid=rb, rh_ask=ra)
            sell_edge = (quote["bid"] / ra - 1.0) * 1e4
            buy_edge = (rb / quote["ask"] - 1.0) * 1e4
            view.ready = True
            view.missing = ""
            view.var_pos, view.rh_pos = var_pos, rh_pos
            view.sell_edge, view.buy_edge = sell_edge, buy_edge
            view.signal = action[0] if action else "-"
            if time.monotonic() - last_status >= 5:
                if var_pos > 0:
                    next_condition = f"close_long:sell_edge>={band.long_exit_bps:+.2f}"
                elif var_pos < 0:
                    next_condition = f"close_short:buy_edge>={band.short_exit_bps:+.2f}"
                else:
                    next_condition = (f"open_short:sell_edge>={band.sell_hurdle_bps:+.2f} "
                                      f"or open_long:buy_edge>={band.buy_hurdle_bps:+.2f}")
                view.next_condition = next_condition
                logging.info("running: sell_edge=%+.2f buy_edge=%+.2f "
                             "var_pos=%+.8g RH_pos=%+.8g signal=%s next=%s "
                             "last_profit=%s total_profit=%+.4f total_volume=$%.2f",
                             sell_edge, buy_edge, var_pos, rh_pos,
                             action[0] if action else "-", next_condition,
                             f"${last_cycle_profit:+.4f}" if last_cycle_profit is not None else "—",
                             total_profit, total_volume)
                last_status = time.monotonic()
            action_name = action[0] if action else None
            if action_name != last_action:
                armed_since = time.monotonic() if action else None
                last_action = action_name
            if not action or armed_since is None or time.monotonic() - armed_since < persist:
                continue
            if time.monotonic() - last_trade < cooldown:
                continue
            is_buy_var = action_name in {"open_long", "close_short"}
            side = "BUY" if is_buy_var else "SELL"
            ref = quote["ask"] if is_buy_var else quote["bid"]
            order_notional = notional
            if not args.live and action_name.startswith("open"):
                # A dry-run may use the venue minimum solely to validate that
                # the page form becomes actionable.  It never submits and it
                # does not silently enlarge the configured live size.
                order_notional = max(notional, minimum, rh.min_quote,
                                     rh.min_base * ref * 1.01)
                if order_notional > notional:
                    logging.info("dry-run form probe uses $%.2f (configured live size remains $%.2f)",
                                 order_notional, notional)
            qty = (abs(var_pos) if action_name.startswith("close")
                   else order_notional / ref)
            scale = 10 ** rh.size_decimals
            qty = ((math.floor(qty * scale + 1e-9) if args.live
                    else math.ceil(qty * scale - 1e-9)) / scale)
            if qty <= 0:
                raise RuntimeError("calculated order quantity rounds to zero")
            if args.live and qty * ref < rh.min_quote:
                raise RuntimeError(f"order {qty:g} {symbol} (${qty * ref:.2f}) is below RH minimum ${rh.min_quote:g}")
            if (args.live and action_name.startswith("open")
                    and (abs(var_pos) * ref + qty * ref > conf.cap_usd)):
                raise RuntimeError("order would exceed configured position cap")
            logging.warning("signal %s edge=%+.2f: Variational %s %.8g", action_name,
                            action[1], side, qty)
            result = await commands.place_order(side=side, quantity=qty, symbol=symbol,
                                                submit=False, timeout=10)
            sized_quote = await _wait_sized_quote(state, symbol, qty)
            sized_action = band.decide(
                position=var_pos, variational_bid=sized_quote["bid"],
                variational_ask=sized_quote["ask"], rh_bid=rh.book.best_bid(),
                rh_ask=rh.book.best_ask())
            if not sized_action or sized_action[0] != action_name:
                logging.warning("signal vanished at prepared size; no order submitted")
                armed_since = None
                last_action = None
                continue
            if not args.live:
                logging.warning("DRY-RUN page probe passed at sized quote: %s", result)
                last_trade = time.monotonic()
                armed_since = None
                while band.decide(position=var_pos, variational_bid=quote["bid"],
                                  variational_ask=quote["ask"], rh_bid=rb, rh_ask=ra):
                    await asyncio.sleep(1)
                    quote = state.quote(symbol) or quote
                    rb, ra = rh.book.best_bid() or rb, rh.book.best_ask() or ra
                continue
            await commands.place_order(side=side, quantity=qty, symbol=symbol,
                                       submit=True, timeout=10)
            filled_qty, trade_id, var_fill_px = await _confirmed_fill(state, symbol, side, fill_timeout)
            hedge_buy = side == "SELL"
            hedge_ref = rh.book.best_ask() if hedge_buy else rh.book.best_bid()
            slippage = float(execution.get("hedge_slippage_bps", 20.0)) / 1e4
            limit = hedge_ref * (1 + slippage if hedge_buy else 1 - slippage)
            limit = rh.px_round(limit, round_up=hedge_buy)
            hedge = await rh.send_taker(is_buy=hedge_buy, qty=filled_qty,
                                        limit_px=limit,
                                        reduce_only=action_name.startswith("close"))
            if hedge.get("err") or hedge.get("unresolved") or hedge.get("filled_base", 0) <= 0:
                raise RuntimeError(f"RH hedge failed after Variational trade {trade_id}: {hedge}; HEDGE MANUALLY")
            hedge_qty = float(hedge.get("filled_base") or 0.0)
            hedge_px = float(hedge.get("avg_px") or hedge_ref)
            var_px = float(var_fill_px or ref)
            cycle_volume = filled_qty * var_px + hedge_qty * hedge_px
            cycle_profit = ((var_px - hedge_px) if side == "SELL" else (hedge_px - var_px)) * min(filled_qty, hedge_qty)
            total_profit += cycle_profit
            total_volume += cycle_volume
            last_cycle_profit = cycle_profit
            last_cycle_volume = cycle_volume
            # RH hedge direction: buying RH increases its base position.
            rh_pos += (hedge_qty if hedge_buy else -hedge_qty)
            view.total_profit, view.total_volume = total_profit, total_volume
            view.last_profit, view.last_volume = cycle_profit, cycle_volume
            view.trades = trades + 1
            view.var_pos, view.rh_pos = _position(state, symbol), rh_pos
            view.recent_trades.append({"time": time.strftime("%H:%M:%S"),
                                       "action": action_name, "qty": min(filled_qty, hedge_qty),
                                       "profit": cycle_profit, "volume": cycle_volume})
            logging.warning("cycle complete: trade=%s qty=%.8g var_px=$%.6f RH_px=$%.6f "
                            "cycle_profit=%+.4f cycle_volume=$%.2f total_profit=%+.4f "
                            "total_volume=$%.2f var_pos=%+.8g RH_pos=%+.8g",
                            trade_id, min(filled_qty, hedge_qty), var_px, hedge_px,
                            cycle_profit, cycle_volume, total_profit, total_volume,
                            _position(state, symbol), rh_pos)
            trades += 1
            last_trade = time.monotonic()
            armed_since = None
        logging.warning("max_trades_per_session=%d reached; stopped", max_trades)
    finally:
        stop.set()
        view.stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await rh.close()
        await bridge.close()
        await commands.close()
        await receiver.close()
        await session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--env-file", default="4.env")
    parser.add_argument("--live", action="store_true",
                        help="send real orders; extension must also be armed")
    parser.add_argument("--no-dashboard", action="store_true",
                        help="disable the Rich terminal dashboard")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as fh:
        main_raw = yaml.safe_load(fh) or {}
    logging_cfg = main_raw.get("logging") or {}
    args.dashboard = bool(logging_cfg.get("dashboard", True)) and not args.no_dashboard and sys.stdout.isatty()
    args.log_file = str(logging_cfg.get("file") or "logs/engine_{symbol}.log")
    symbol = str((main_raw.get("analysis") or {}).get("symbol") or "VAR")
    args.log_file = args.log_file.format(symbol=symbol)
    args.log_buffer = BufferLogHandler()
    root = logging.getLogger()
    root.setLevel(getattr(logging, str(logging_cfg.get("level", "INFO")).upper(), logging.INFO))
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    if args.dashboard:
        os.makedirs(os.path.dirname(args.log_file) or ".", exist_ok=True)
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        root.addHandler(args.log_buffer)
    else:
        stream = logging.StreamHandler()
        stream.setFormatter(formatter)
        root.addHandler(stream)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        raise SystemExit(str(error))


if __name__ == "__main__":
    main()
