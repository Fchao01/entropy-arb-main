#!/usr/bin/env python3
"""Hedge confirmed manual Variational fills on Lighter.

This intentionally follows variational-v1's operating model: the user places
the Variational leg in the authenticated browser; this process observes the
confirmed fill through the allowlisted CDP forwarder and sends the opposite
Lighter IOC.  It does not click or place a Variational order.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from entropy_arb.config import LIGHTER_PROFILES, LighterCreds, VenueConf
from entropy_arb.variational import (VariationalReceiver, VariationalState,
                                     confirmed_hedge_instruction,
                                     normalize_proxy_url)
from entropy_arb.venue_lighter import LighterVenue


def credentials(profile: str) -> LighterCreds:
    prefix = "LIGHTER_RH" if profile == "lighter-rh" else "LIGHTER"
    account = os.getenv(f"{prefix}_ACCOUNT_INDEX") or os.getenv("LIGHTER_ACCOUNT_INDEX")
    key_index = os.getenv(f"{prefix}_API_KEY_INDEX") or os.getenv("LIGHTER_API_KEY_INDEX")
    private = (os.getenv(f"{prefix}_API_PRIVATE_KEY")
               or os.getenv("LIGHTER_API_PRIVATE_KEY")
               or os.getenv("LIGHTER_PRIVATE_KEY"))
    return LighterCreds(int(account) if account else None,
                        int(key_index) if key_index else None, private)


async def run(args) -> None:
    if not args.live:
        raise RuntimeError("refusing to send hedge orders without --live")
    proxy = normalize_proxy_url(args.proxy)
    if proxy:
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
    load_dotenv(args.env_file)
    creds = credentials(args.hedge)
    if not creds.complete:
        raise RuntimeError("incomplete Lighter credentials in .env")

    conf = VenueConf(
        key="lighter", kind="lighter",
        label="RH" if args.hedge == "lighter-rh" else "LIGHTER",
        symbol=args.symbol.upper(), fee_bps=args.fee_bps,
        cap_usd=args.max_position_usd, orders_per_min=args.max_orders_per_min,
        lighter_profile=LIGHTER_PROFILES[args.hedge], lighter_creds=creds)
    stop, update = asyncio.Event(), asyncio.Event()
    state = VariationalState()
    receiver = VariationalReceiver(state, ws_port=args.ws_port,
                                   rest_port=args.rest_port)
    session = aiohttp.ClientSession(trust_env=True)
    venue = LighterVenue(conf, session, args.settle_timeout)
    tasks = []
    processed = set()
    try:
        await receiver.start()
        await venue.load_market()
        venue.init_signer()
        tasks = venue.start_tasks(stop, update.set, live=True)
        logging.warning("LIVE manual-Variational -> %s hedge armed for %s",
                        venue.name, conf.symbol)
        while not stop.is_set():
            event = await state.trade_events.get()
            instruction = confirmed_hedge_instruction(event, conf.symbol)
            if instruction is None:
                continue
            is_buy, qty, trade_id = instruction
            if trade_id in processed:
                continue
            processed.add(trade_id)
            if qty < venue.min_base:
                logging.error("trade %s qty %.8g is below Lighter minimum",
                              trade_id, qty)
                continue
            if not venue.ready_to_trade() or not venue.book.is_fresh(args.staleness):
                logging.critical("trade %s confirmed but Lighter is not ready; "
                                 "HEDGE MANUALLY", trade_id)
                continue
            ref = venue.book.best_ask() if is_buy else venue.book.best_bid()
            factor = 1 + args.slippage_bps / 1e4 if is_buy else 1 - args.slippage_bps / 1e4
            limit = venue.px_round(ref * factor, round_up=is_buy)
            logging.warning("hedging Variational %s: %s %.8g %s @ %.8g",
                            trade_id, "BUY" if is_buy else "SELL", qty,
                            venue.name, limit)
            result = await venue.send_taker(is_buy=is_buy, qty=qty,
                                            limit_px=limit)
            if result.get("err") or result.get("unresolved"):
                logging.critical("hedge failed/unknown for %s: %s; HEDGE MANUALLY",
                                 trade_id, result)
            else:
                logging.warning("hedge result %s: %s", trade_id, result)
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await venue.close()
        await receiver.close()
        await session.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", required=True)
    p.add_argument("--hedge", choices=tuple(LIGHTER_PROFILES), default="lighter")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--live", action="store_true",
                   help="required acknowledgement that real Lighter orders are sent")
    p.add_argument("--ws-port", type=int, default=8766)
    p.add_argument("--rest-port", type=int, default=8767)
    p.add_argument("--slippage-bps", type=float, default=100.0)
    p.add_argument("--settle-timeout", type=float, default=10.0)
    p.add_argument("--staleness", type=float, default=10.0)
    p.add_argument("--fee-bps", type=float, default=0.0)
    p.add_argument("--max-position-usd", type=float, default=1000.0)
    p.add_argument("--max-orders-per-min", type=int, default=30)
    p.add_argument("--proxy", default="",
                   help="HTTP(S) proxy, e.g. http://127.0.0.1:7897")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except RuntimeError as error:
        raise SystemExit(str(error))


if __name__ == "__main__":
    main()
