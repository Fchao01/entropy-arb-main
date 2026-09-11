#!/usr/bin/env python3
"""Run the local receiver expected by the Variational Chrome extension."""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from entropy_arb.variational import VariationalReceiver, VariationalState


async def run(args) -> None:
    state = VariationalState()
    receiver = VariationalReceiver(state, args.host, args.ws_port, args.rest_port)
    await receiver.start()
    print(f"Variational receiver ready: ws://{args.host}:{args.ws_port} and "
          f"ws://{args.host}:{args.rest_port}", flush=True)
    try:
        while True:
            await asyncio.sleep(args.status_interval)
            print(json.dumps({"fresh": state.fresh,
                              "channels": sorted(state.connected_channels),
                              "quotes": state.quotes,
                              "positions": state.positions}, ensure_ascii=False),
                  flush=True)
    finally:
        await receiver.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--ws-port", type=int, default=8766)
    p.add_argument("--rest-port", type=int, default=8767)
    p.add_argument("--status-interval", type=float, default=5.0)
    args = p.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
