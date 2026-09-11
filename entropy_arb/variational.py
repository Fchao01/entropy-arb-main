"""Variational browser-forwarder receiver.

The Variational web application owns authentication and wallet state.  A
Chrome extension attaches to its tab with ``chrome.debugger`` and forwards a
small allowlist of CDP REST responses and websocket frames to these localhost
receivers.  This module deliberately cannot click the UI or submit an order.
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

import websockets

QUOTE_PATH = "/api/quotes/indicative"
EVENTS_PATH = "/events"
PORTFOLIO_PATH = "/portfolio"


def canonical_symbol(value: str) -> str:
    """Normalize common Variational display symbols (e.g. XAUUSD -> XAU)."""
    symbol = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    return {"XAUS": "XAU", "XAUUSD": "XAU", "GOLD": "XAU"}.get(symbol, symbol)


def normalize_proxy_url(value: str) -> str:
    """Accept a plain proxy URL or Markdown's ``[url](url)`` rendering."""
    value = (value or "").strip()
    match = re.fullmatch(r"\[(https?://[^\]]+)\]\((https?://[^)]+)\)", value)
    if match:
        value = match.group(2)
    if value and not re.fullmatch(r"https?://[^\s]+", value):
        raise ValueError("proxy must look like http://127.0.0.1:7897")
    return value


@dataclass(frozen=True)
class VariationalBand:
    """Band decision for Variational (primary) against Lighter RH.

    ``position`` is the Variational base position: positive means long and
    negative means short. Existing positions close when the executable spread
    returns to the measured midline; entry bands are only used when flat.
    """

    midline_bps: float = 2.0
    upper_bps: float = 11.0
    lower_bps: float = 8.5

    @property
    def sell_hurdle_bps(self) -> float:
        return self.midline_bps + self.upper_bps

    @property
    def buy_hurdle_bps(self) -> float:
        return self.lower_bps - self.midline_bps

    @property
    def long_exit_bps(self) -> float:
        """SELL-primary edge needed to exit a Variational long."""
        return self.midline_bps

    @property
    def short_exit_bps(self) -> float:
        """BUY-primary edge needed to exit a Variational short."""
        return -self.midline_bps

    def decide(self, *, position: float, variational_bid: float,
               variational_ask: float, rh_bid: float, rh_ask: float):
        sell_edge = (variational_bid / rh_ask - 1.0) * 1e4
        buy_edge = (rh_bid / variational_ask - 1.0) * 1e4
        if position > 0:
            return ("close_long", sell_edge) if sell_edge >= self.long_exit_bps else None
        if position < 0:
            return ("close_short", buy_edge) if buy_edge >= self.short_exit_bps else None
        candidates = []
        if sell_edge >= self.sell_hurdle_bps:
            candidates.append((sell_edge - self.sell_hurdle_bps,
                               "open_short", sell_edge))
        if buy_edge >= self.buy_hurdle_bps:
            candidates.append((buy_edge - self.buy_hurdle_bps,
                               "open_long", buy_edge))
        if not candidates:
            return None
        _, action, edge = max(candidates)
        return action, edge


def confirmed_hedge_instruction(event: dict[str, Any], symbol: str):
    """Return ``(lighter_is_buy, qty, trade_id)`` for a confirmed fill."""
    if str(event.get("status", "")).strip().lower() != "confirmed":
        return None
    instrument = event.get("instrument")
    asset = canonical_symbol(instrument.get("underlying", "") if isinstance(instrument, dict)
                             else "")
    if asset != symbol.strip().upper():
        return None
    side = str(event.get("side", "")).strip().lower()
    if side not in {"buy", "sell"}:
        return None
    try:
        qty = float(event.get("qty"))
    except (TypeError, ValueError):
        return None
    trade_id = str(event.get("id", "")).strip()
    if qty <= 0 or not trade_id:
        return None
    return side == "sell", qty, trade_id


def _json(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def _path(url: str) -> str:
    try:
        return urlparse(url).path
    except ValueError:
        return ""


def _body(payload: dict) -> Optional[str]:
    value = payload.get("body")
    if not isinstance(value, str):
        return None
    if not payload.get("base64Encoded"):
        return value
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return None


def _frame(payload: dict) -> Optional[str]:
    value = payload.get("payloadData")
    if not isinstance(value, str):
        return None
    if payload.get("opcode") != 2 or value.lstrip().startswith(("{", "[")):
        return value
    try:
        return base64.b64decode(value).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return value


@dataclass
class VariationalState:
    """Latest state observed in one authenticated Variational browser tab."""

    quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    portfolio: dict[str, Any] = field(default_factory=dict)
    last_heartbeat: float = 0.0
    last_update: float = 0.0
    connected_channels: set[str] = field(default_factory=set)
    trade_events: asyncio.Queue = field(default_factory=asyncio.Queue, repr=False)
    _seen_trade_states: set[tuple[str, str]] = field(default_factory=set,
                                                        repr=False)

    @property
    def fresh(self) -> bool:
        return self.last_heartbeat > 0 and time.monotonic() - self.last_heartbeat <= 11

    def quote(self, symbol: str) -> Optional[dict[str, Any]]:
        return self.quotes.get(canonical_symbol(symbol))

    async def accept_rest(self, payload: dict[str, Any]) -> None:
        if payload.get("kind") != "rest_response" or _path(str(payload.get("url", ""))) != QUOTE_PATH:
            return
        parsed = _json(_body(payload))
        if not isinstance(parsed, dict):
            return
        instrument = parsed.get("instrument")
        if not isinstance(instrument, dict):
            return
        symbol = canonical_symbol(instrument.get("underlying", ""))
        try:
            bid, ask = float(parsed["bid"]), float(parsed["ask"])
        except (KeyError, TypeError, ValueError):
            return
        if not symbol or bid <= 0 or ask <= 0 or bid > ask:
            return
        self.quotes[symbol] = {"bid": bid, "ask": ask,
                               "mark_price": parsed.get("mark_price"),
                               "timestamp": parsed.get("timestamp"), "raw": parsed}
        self.last_update = time.monotonic()

    async def accept_ws(self, payload: dict[str, Any]) -> None:
        if payload.get("kind") == "ws_closed":
            self.connected_channels.discard(_path(str(payload.get("url", ""))))
            return
        if payload.get("kind") != "ws_frame" or payload.get("direction") != "received":
            return
        channel = _path(str(payload.get("url", "")))
        if channel not in {EVENTS_PATH, PORTFOLIO_PATH}:
            return
        parsed = _json(_frame(payload))
        if parsed is None:
            return
        self.connected_channels.add(channel)
        self.last_update = time.monotonic()
        if channel == PORTFOLIO_PATH:
            self._accept_portfolio(parsed)
            return
        events = parsed if isinstance(parsed, list) else [parsed]
        if isinstance(parsed, dict):
            events += [x for x in parsed.get("events", []) if isinstance(x, dict)]
            if isinstance(parsed.get("data"), list):
                events += [x for x in parsed["data"] if isinstance(x, dict)]
        for event in events:
            if not isinstance(event, dict):
                continue
            if event.get("type") == "heartbeat":
                self.last_heartbeat = time.monotonic()
            if "trade" in str(event.get("type", "")).lower():
                data = event.get("data") if isinstance(event.get("data"), dict) else event
                trade_id = str(data.get("id", ""))
                status = str(data.get("status", "")).lower()
                key = (trade_id, status)
                if trade_id and key not in self._seen_trade_states:
                    self._seen_trade_states.add(key)
                    await self.trade_events.put(data)

    def _accept_portfolio(self, parsed: Any) -> None:
        if not isinstance(parsed, dict):
            return
        positions: dict[str, dict[str, Any]] = {}
        for row in parsed.get("positions", []):
            info = row.get("position_info", {}) if isinstance(row, dict) else {}
            instrument = info.get("instrument", {}) if isinstance(info, dict) else {}
            symbol = canonical_symbol(instrument.get("underlying", ""))
            if symbol:
                positions[symbol] = {"qty": info.get("qty"),
                                     "avg_entry_price": info.get("avg_entry_price"),
                                     "upnl": row.get("upnl"), "raw": row}
        self.positions = positions
        pool = parsed.get("pool_portfolio_result")
        self.portfolio = pool if isinstance(pool, dict) else {}


class VariationalReceiver:
    """Two localhost websocket servers compatible with the reference extension."""

    def __init__(self, state: VariationalState, host: str = "127.0.0.1",
                 ws_port: int = 8766, rest_port: int = 8767) -> None:
        self.state = state
        self.host, self.ws_port, self.rest_port = host, ws_port, rest_port
        self._servers = []

    async def start(self) -> None:
        async def serve(handler: Callable[[dict], Awaitable[None]], socket) -> None:
            async for raw in socket:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                payload = _json(raw)
                if isinstance(payload, dict):
                    await handler(payload)

        self._servers = [
            await websockets.serve(lambda ws: serve(self.state.accept_ws, ws),
                                   self.host, self.ws_port, max_size=None),
            await websockets.serve(lambda ws: serve(self.state.accept_rest, ws),
                                   self.host, self.rest_port, max_size=None),
        ]

    async def close(self) -> None:
        for server in self._servers:
            server.close()
        await asyncio.gather(*(server.wait_closed() for server in self._servers))
        self._servers.clear()


class VariationalCommandServer:
    """Local command connection for the Chrome extension.

    A successful result confirms the page interaction only.  It is never a
    fill confirmation; callers must wait for a ``confirmed`` trade event.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8768) -> None:
        self.host, self.port = host, port
        self.connected = asyncio.Event()
        self._server = None
        self._socket = None
        self._pending: dict[str, asyncio.Future] = {}
        self._sequence = 0

    async def start(self) -> None:
        async def handler(socket) -> None:
            old = self._socket
            if old is not None and old is not socket:
                await old.close()
            self._socket = socket
            self.connected.set()
            try:
                async for raw in socket:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    payload = _json(raw)
                    if not isinstance(payload, dict):
                        continue
                    request_id = str(payload.get("requestId", ""))
                    future = self._pending.pop(request_id, None)
                    if future is not None and not future.done():
                        future.set_result(payload)
            finally:
                if self._socket is socket:
                    self._socket = None
                    self.connected.clear()

        self._server = await websockets.serve(handler, self.host, self.port,
                                              max_size=None)

    async def place_order(self, *, side: str, quantity: float, symbol: str,
                          submit: bool, timeout: float = 10.0) -> dict[str, Any]:
        if self._socket is None:
            raise RuntimeError("Variational extension command channel is disconnected")
        self._sequence += 1
        request_id = f"var-{int(time.time() * 1000)}-{self._sequence}"
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        payload = {"type": "PLACE_ORDER", "requestId": request_id,
                   "side": side.upper(), "quantity": format(quantity, ".12g"),
                   "symbol": symbol.upper(), "submit": bool(submit)}
        try:
            await self._socket.send(json.dumps(payload))
            result = await asyncio.wait_for(future, timeout=timeout)
        except BaseException:
            self._pending.pop(request_id, None)
            raise
        if not result.get("ok"):
            raise RuntimeError(str(result.get("error") or
                                   "Variational page command failed"))
        return result

    async def close(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
        if self._socket is not None:
            await self._socket.close()
        self._socket = None
        self.connected.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        self._server = None
