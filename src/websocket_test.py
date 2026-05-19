# websocket_test.py
import os
import asyncio
import aiohttp
from aiohttp import client_exceptions

async def test():
    hosts_env = os.getenv("BYBIT_WS_HOSTS", "")
    if hosts_env:
        hosts = [h.strip() for h in hosts_env.split(",") if h.strip()]
    else:
        mainnet = os.getenv("MAINNET", "1").strip().lower() in ("1","true","yes","y")
        host = "stream.bybit.com" if mainnet else "stream-testnet.bybit.com"
        hosts = [f"wss://{host}/realtime_public", f"wss://{host}/realtime"]
    print("Testing WS hosts:", hosts)
    for url in hosts:
        try:
            print("-> Trying", url)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, timeout=10) as ws:
                    print("Connected OK to", url)
                    await ws.close()
        except client_exceptions.WSServerHandshakeError as e:
            print("HandshakeError for", url, ":", repr(e))
        except Exception as e:
            print("Other error for", url, ":", repr(e))

if __name__ == "__main__":
    asyncio.run(test())
