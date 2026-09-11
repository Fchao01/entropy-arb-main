import asyncio
import base64
import json
import socket

import websockets

from entropy_arb.variational import (VariationalBand, VariationalCommandServer,
                                     VariationalReceiver, VariationalState,
                                     confirmed_hedge_instruction,
                                     normalize_proxy_url)


def test_normalize_proxy_url():
    url = "http://127.0.0.1:7897"
    assert normalize_proxy_url(url) == url
    assert normalize_proxy_url(f"[{url}]({url})") == url


def test_variational_xaus_symbol_maps_to_rh_xau():
    from entropy_arb.variational import canonical_symbol

    assert canonical_symbol("XAUS") == "XAU"
    assert canonical_symbol("XAU-USD") == "XAU"


def test_variational_band_open_and_close():
    band = VariationalBand(midline_bps=2.0, upper_bps=11.0, lower_bps=8.5)
    assert band.sell_hurdle_bps == 13.0
    assert band.buy_hurdle_bps == 6.5

    # Variational bid is 15 bps above the executable RH ask.
    assert band.decide(position=0, variational_bid=100.15,
                       variational_ask=100.17, rh_bid=99.98,
                       rh_ask=100.0)[0] == "open_short"
    # That signal closes a Variational long, but cannot reverse it at once.
    assert band.decide(position=1, variational_bid=100.15,
                       variational_ask=100.17, rh_bid=99.98,
                       rh_ask=100.0)[0] == "close_long"

    # RH bid is 10 bps above the executable Variational ask.
    assert band.decide(position=0, variational_bid=99.88,
                       variational_ask=99.90, rh_bid=100.0,
                       rh_ask=100.02)[0] == "open_long"
    assert band.decide(position=-1, variational_bid=99.88,
                       variational_ask=99.90, rh_bid=100.0,
                       rh_ask=100.02)[0] == "close_short"

    assert band.decide(position=0, variational_bid=99.99,
                       variational_ask=100.01, rh_bid=99.99,
                       rh_ask=100.01) is None


def test_existing_positions_exit_at_midline_not_entry_extreme():
    band = VariationalBand(midline_bps=35.1, upper_bps=18.5, lower_bps=34.5)
    # A long exits after the executable sell edge returns to the midline;
    # it must not wait for the 53.6 bps short-entry hurdle.
    assert band.decide(position=1, variational_bid=100.352,
                       variational_ask=100.40, rh_bid=100.0,
                       rh_ask=100.0)[0] == "close_long"
    # A short exits when the inverse executable edge reaches -midline.
    assert band.decide(position=-1, variational_bid=100.0,
                       variational_ask=100.0, rh_bid=99.650,
                       rh_ask=100.0)[0] == "close_short"


def test_confirmed_hedge_instruction():
    event = {"id": "t1", "status": "confirmed", "side": "buy",
             "qty": "0.25", "instrument": {"underlying": "BTC"}}
    assert confirmed_hedge_instruction(event, "BTC") == (False, 0.25, "t1")
    event["side"] = "sell"
    assert confirmed_hedge_instruction(event, "btc") == (True, 0.25, "t1")
    event["status"] = "pending"
    assert confirmed_hedge_instruction(event, "BTC") is None


def test_quote_response():
    async def go():
        state = VariationalState()
        body = {"instrument": {"underlying": "BTC"}, "bid": "100.1",
                "ask": "100.2", "mark_price": "100.15"}
        await state.accept_rest({"kind": "rest_response",
                                 "url": "https://omni.variational.io/api/quotes/indicative?x=1",
                                 "body": json.dumps(body), "base64Encoded": False})
        assert state.quote("btc")["bid"] == 100.1
        assert state.quote("BTC")["ask"] == 100.2
    asyncio.run(go())


def test_binary_trade_and_portfolio_frames():
    async def go():
        state = VariationalState()
        trade = {"type": "trade_update", "data": {"id": "t1", "side": "buy",
                 "qty": "0.1", "price": "100", "status": "confirmed",
                 "instrument": {"underlying": "BTC"}}}
        encoded = base64.b64encode(json.dumps(trade).encode()).decode()
        await state.accept_ws({"kind": "ws_frame", "direction": "received",
                               "url": "wss://example.variational.io/events",
                               "opcode": 2, "payloadData": encoded})
        assert (await state.trade_events.get())["id"] == "t1"
        # Duplicate lifecycle states are ignored.
        await state.accept_ws({"kind": "ws_frame", "direction": "received",
                               "url": "wss://example.variational.io/events",
                               "opcode": 2, "payloadData": encoded})
        assert state.trade_events.empty()

        portfolio = {"positions": [{"position_info": {
            "instrument": {"underlying": "BTC"}, "qty": "0.25",
            "avg_entry_price": "99"}, "upnl": "1.2"}],
            "pool_portfolio_result": {"balance": "1000"}}
        await state.accept_ws({"kind": "ws_frame", "direction": "received",
                               "url": "wss://example.variational.io/portfolio",
                               "opcode": 1, "payloadData": json.dumps(portfolio)})
        assert state.positions["BTC"]["qty"] == "0.25"
        assert state.portfolio["balance"] == "1000"
    asyncio.run(go())


def test_receiver_end_to_end():
    def free_port():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    async def go():
        ws_port, rest_port = free_port(), free_port()
        state = VariationalState()
        receiver = VariationalReceiver(state, ws_port=ws_port,
                                       rest_port=rest_port)
        await receiver.start()
        try:
            async with websockets.connect(f"ws://127.0.0.1:{rest_port}") as ws:
                body = {"instrument": {"underlying": "ETH"},
                        "bid": "1999", "ask": "2001"}
                await ws.send(json.dumps({
                    "kind": "rest_response",
                    "url": "https://omni.variational.io/api/quotes/indicative",
                    "body": json.dumps(body), "base64Encoded": False}))
            for _ in range(20):
                if state.quote("ETH"):
                    break
                await asyncio.sleep(0.01)
            assert state.quote("ETH") == {
                "bid": 1999.0, "ask": 2001.0, "mark_price": None,
                "timestamp": None, "raw": body}
        finally:
            await receiver.close()
    asyncio.run(go())


def test_command_server_round_trip():
    async def go():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        server = VariationalCommandServer(port=port)
        await server.start()
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}") as ws:
                async def extension():
                    command = json.loads(await ws.recv())
                    assert command["side"] == "BUY"
                    assert command["submit"] is False
                    await ws.send(json.dumps({"requestId": command["requestId"],
                                              "ok": True, "submitted": False}))
                task = asyncio.create_task(extension())
                await server.connected.wait()
                result = await server.place_order(side="buy", quantity=0.01,
                                                  symbol="ETH", submit=False)
                assert result["ok"] is True
                await task
        finally:
            await server.close()
    asyncio.run(go())
