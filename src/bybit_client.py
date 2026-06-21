# bybit_client.py
import os
import asyncio
import json
import time
import traceback
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque

import aiohttp
from aiohttp import client_exceptions

from .config import MAINNET, BYBIT_API_KEY, BYBIT_API_SECRET, RATE_LIMIT_RPS, KLINE_SEED_LIMIT
from .logger import get_logger
from .ratelimiter import TokenBucket

logger = get_logger("bybit_client")


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "y")


class BybitClient:
    def __init__(self, rate_limiter: Optional[TokenBucket] = None):
        try:
            mainnet_flag = MAINNET.strip().lower() in ("1", "true", "yes", "y") if isinstance(MAINNET, str) else bool(MAINNET)
        except Exception:
            mainnet_flag = True

        self.rest_base = "https://api.bybit.com" if mainnet_flag else "https://api-testnet.bybit.com"
        host = "stream.bybit.com" if mainnet_flag else "stream-testnet.bybit.com"

        env_hosts = os.getenv("BYBIT_WS_HOSTS", "")
        extra = [h.strip() for h in env_hosts.split(",") if h.strip()]
        default_ws_hosts = [
            f"wss://{host}/v5/public/linear",
            f"wss://{host}/v5/private",
        ]
        self.ws_hosts = extra + default_ws_hosts

        logger.info("BybitClient rest_base=%s ws_hosts=%s", self.rest_base, self.ws_hosts)

        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self._session: Optional[aiohttp.ClientSession] = None
        self._max_retries = 3
        self._backoff_base = 1.0
        try:
            rate_val = float(RATE_LIMIT_RPS)
        except Exception:
            rate_val = 5.0
        self.rate_limiter = rate_limiter or TokenBucket(max(1.0, rate_val))

        # WS state
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_session: Optional[aiohttp.ClientSession] = None
        self._pending_subscribe: asyncio.Queue = asyncio.Queue()
        self._kline_cache: Dict[str, Dict[str, deque]] = defaultdict(dict)
        self._requested_subs: set = set()
        self._mtf_subscribed: set = set()
        self._ws_stop = False
        self._ws_backoff = 1.0

    async def _session_obj(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        await self.stop_kline_ws()
        if self._session:
            try:
                await self._session.close()
            except Exception:
                logger.exception("Error closing REST session")
            self._session = None

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12) -> Optional[Dict[str, Any]]:
        session = await self._session_obj()
        url = self.rest_base + path
        for attempt in range(self._max_retries):
            await self.rate_limiter.acquire()
            try:
                async with session.get(url, params=params, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    if status == 429 or (500 <= status < 600):
                        wait = self._backoff_base * (2 ** attempt)
                        logger.warning("HTTP %s from %s â€” backoff %.1fs (attempt %d/%d)", status, url, wait, attempt + 1, self._max_retries)
                        await asyncio.sleep(wait)
                        continue
                    try:
                        data = await resp.json()
                    except Exception:
                        snippet = (text[:2000] + '...') if len(text) > 2000 else text
                        logger.warning("Bybit returned non-JSON (status=%s) from %s. Body:\n%s", status, url, snippet)
                        return None
                    if status >= 400:
                        snippet = (text[:400] + '...') if len(text) > 400 else text
                        logger.error("Bybit GET %s returned %s: %s", url, status, snippet)
                        return None
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt + 1 >= self._max_retries:
                    logger.exception("GET %s failed after %d attempts: %s", url, attempt + 1, e)
                    return None
                wait = self._backoff_base * (2 ** attempt)
                logger.warning("Request error for %s: %s. Retrying in %.1fs (attempt %d/%d)", url, e, wait, attempt + 1, self._max_retries)
                await asyncio.sleep(wait)
        return None

    # ----- get_klines with ALWAYS-ON detailed logging -----
    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> Optional[Any]:
        """
        Get klines with detailed always-on logging to console.
        Every request and response is logged so you can see exactly what's happening.
        """
        tried = []
        variants = []
        s = str(interval).strip().lower()
        if s.endswith("m") or s.endswith("h") or s.endswith("d"):
            variants.append(s)
            try:
                if s.endswith("m"):
                    variants.append(str(int(s[:-1])))
            except Exception:
                pass
        else:
            variants.append(s)
            try:
                variants.append(f"{int(s)}m")
            except Exception:
                pass
        if s in ("1d", "d", "day"):
            variants.append("D")
        seen = set()
        variants = [v for v in variants if not (v in seen or seen.add(v))]

        endpoints = [
            ("/v5/market/kline", "v5"),
            ("/v2/public/kline/list", "v2")
        ]

        for ep, tag in endpoints:
            for iv in variants:
                tried.append((ep, iv))
                try:
                    params = {"symbol": symbol, "interval": iv, "limit": limit}
                    logger.info("=== get_klines: REQUEST symbol=%s interval=%s endpoint=%s params=%s", symbol, iv, ep, params)
                    data = await self._get(ep, params=params)
                    try:
                        txt = json.dumps(data, default=str)
                    except Exception:
                        txt = str(data)
                    snippet = txt if len(txt) <= 2000 else (txt[:2000] + " ...[truncated]")
                    logger.info("=== get_klines: RESPONSE symbol=%s interval=%s endpoint=%s len=%d body=%s", symbol, iv, ep, len(txt) if isinstance(txt, str) else 0, snippet)
                    
                    if not data:
                        logger.info("=== get_klines: empty data, trying next variant")
                        continue

                    if isinstance(data, dict):
                        if "ret_code" in data and data.get("ret_code", 0) == 0 and "result" in data:
                            res = data["result"]
                            if isinstance(res, dict) and isinstance(res.get("list"), list):
                                item_count = len(res.get("list", []))
                                logger.info("=== get_klines: SUCCESS (v5 dict.result.list) returning %d items", item_count)
                                return res.get("list", [])
                            if isinstance(res, list):
                                logger.info("=== get_klines: SUCCESS (v5 dict.result is list) returning %d items", len(res))
                                return res
                            if isinstance(res, (list, tuple)):
                                logger.info("=== get_klines: SUCCESS (v5 dict.result is tuple) returning %d items", len(res))
                                return list(res)
                            logger.info("=== get_klines: SUCCESS (v5 dict.result raw)")
                            return res
                        if "result" in data:
                            logger.info("=== get_klines: returning dict['result']")
                            return data["result"]
                        if isinstance(data, list):
                            logger.info("=== get_klines: SUCCESS (dict is list) returning %d items", len(data))
                            return data
                        logger.info("=== get_klines: returning dict as-is")
                        return data
                    if isinstance(data, (list, tuple)):
                        logger.info("=== get_klines: SUCCESS (top-level list/tuple) returning %d items", len(data))
                        return list(data)
                except Exception:
                    logger.exception("=== get_klines: EXCEPTION attempt failed for %s %s @ %s (tag=%s)", symbol, iv, ep, tag)
                    continue

        logger.info("=== get_klines: FAILED all attempts. no usable klines for %s interval variants=%s tried=%s", symbol, variants, tried)
        return None

    async def get_latest_price(self, symbol: str) -> Optional[float]:
        try:
            params = {"symbol": symbol}
            data = await self._get("/v5/market/tickers", params=params)
            if isinstance(data, dict) and "result" in data:
                res = data["result"]
                if isinstance(res, list) and len(res) > 0:
                    entry = res[0]
                elif isinstance(res, dict) and "list" in res and isinstance(res["list"], list) and len(res["list"]) > 0:
                    entry = res["list"][0]
                elif isinstance(res, dict):
                    entry = res
                else:
                    entry = None
                if entry:
                    for k in ("lastPrice", "last_price", "last", "price"):
                        if k in entry and entry[k] is not None:
                            try:
                                return float(entry[k])
                            except Exception:
                                continue
            data2 = await self._get("/v2/public/tickers", params={"symbol": symbol})
            if isinstance(data2, dict) and "result" in data2:
                res = data2["result"]
                if isinstance(res, list) and len(res) > 0:
                    entry = res[0]
                    if "last_price" in entry:
                        try:
                            return float(entry["last_price"])
                        except Exception:
                            pass
        except Exception:
            logger.exception("get_latest_price error for %s", symbol)
        return None

    async def get_24h_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch 24h ticker stats for a symbol.
        Returns a dict with at minimum:
          - volume24h      : float  (base-asset volume over last 24 h)
          - turnover24h    : float  (quote-asset turnover over last 24 h)
          - price24hPcnt   : float  (price change % as decimal, e.g. 0.023 = +2.3 %)
          - volume24hPcnt  : float  (volume change % vs prior 24 h window — synthesised
                                     from prevVolume24h when available, else None)
        Returns None on failure.
        """
        try:
            params = {"symbol": symbol, "category": "linear"}
            data = await self._get("/v5/market/tickers", params=params)
            if isinstance(data, dict) and "result" in data:
                res = data["result"]
                entry = None
                if isinstance(res, dict) and "list" in res and isinstance(res["list"], list) and res["list"]:
                    entry = res["list"][0]
                elif isinstance(res, list) and res:
                    entry = res[0]
                elif isinstance(res, dict):
                    entry = res

                if entry and isinstance(entry, dict):
                    def _f(v):
                        try:
                            return float(v) if v is not None else None
                        except Exception:
                            return None

                    vol24h     = _f(entry.get("volume24h") or entry.get("volume") or entry.get("vol24h"))
                    turnover24h = _f(entry.get("turnover24h") or entry.get("turnover"))
                    price_pct  = _f(entry.get("price24hPcnt") or entry.get("priceChangePercent"))
                    prev_vol   = _f(entry.get("prevVolume24h") or entry.get("volume24h_prev"))

                    vol_pct = None
                    if vol24h is not None and prev_vol is not None and prev_vol > 0:
                        vol_pct = (vol24h - prev_vol) / prev_vol

                    return {
                        "symbol":        symbol,
                        "volume24h":     vol24h,
                        "turnover24h":   turnover24h,
                        "price24hPcnt":  price_pct,
                        "volume24hPcnt": vol_pct,
                        "prevVolume24h": prev_vol,
                        "raw":           entry,
                    }
        except Exception:
            logger.exception("get_24h_ticker error for %s", symbol)
        return None

    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        try:
            syms = await self.get_symbols()
            if not syms:
                return {}
            target = None
            for it in syms:
                try:
                    if isinstance(it, str) and it.upper() == symbol.upper():
                        target = it
                        break
                    if isinstance(it, dict):
                        name = it.get("name") or it.get("symbol") or it.get("symbolName") or it.get("instrument_name")
                        if name and name.upper() == symbol.upper():
                            target = it
                            break
                        base = it.get("baseCoin") or it.get("base")
                        quote = it.get("quoteCoin") or it.get("quote")
                        if base and quote and f"{base}{quote}".upper() == symbol.upper():
                            target = it
                            break
                except Exception:
                    continue
            if not target:
                return {}

            info: Dict[str, Any] = {}
            if isinstance(target, dict):
                step = None
                min_qty = None
                for key in ("lotSizeFilter", "lot_size_filter", "qty_filter", "quantity_filter"):
                    filt = target.get(key)
                    if isinstance(filt, dict):
                        step = step or filt.get("qtyStep") or filt.get("step") or filt.get("minQty")
                        min_qty = min_qty or filt.get("minQty")
                step = step or target.get("qty_step") or target.get("step") or target.get("quantity_step") or target.get("tick_size")
                min_qty = min_qty or target.get("min_trading_qty") or target.get("min_qty") or target.get("minOrderQty") or target.get("lot_size")
                try:
                    if step is not None:
                        step = float(step)
                except Exception:
                    step = None
                try:
                    if min_qty is not None:
                        min_qty = float(min_qty)
                except Exception:
                    min_qty = None

                info["step"] = step
                info["min_qty"] = min_qty
                info["raw"] = target
            return info
        except Exception:
            logger.exception("get_symbol_info failed for %s", symbol)
            return {}

    async def get_symbols(self) -> List[Dict[str, Any]]:
        try:
            all_instruments = []
            cursor = None

            while True:
                params = {
                    "category": "linear",
                    "instrumentType": "PERPETUAL",
                    "limit": 1000
                }

                if cursor:
                    params["cursor"] = cursor

                data = await self._get("/v5/market/instruments-info", params=params)

                if not isinstance(data, dict):
                    break

                result = data.get("result", {})
                instruments = result.get("list", [])

                if instruments:
                    all_instruments.extend(instruments)

                cursor = result.get("nextPageCursor")

                logger.info(
                    "Fetched page: instruments=%d total=%d cursor=%s",
                    len(instruments),
                    len(all_instruments),
                    cursor
                )

                if not cursor:
                    break

            if all_instruments:
                logger.info("Found %d total instruments via paginated v5", len(all_instruments))
                return all_instruments

        except Exception:
            logger.debug("v5 instruments-info attempt failed", exc_info=True)
        try:
            data = await self._get("/v2/public/symbols")
            if isinstance(data, dict) and "result" in data:
                symbols = data["result"] or []
                logger.info("Found %d symbols via v2", len(symbols))
                return symbols
            logger.debug("v2 symbols returned unexpected payload.")
        except Exception:
            logger.debug("v2 symbols attempt failed", exc_info=True)
        logger.warning("No symbols retrieved from Bybit; returning empty list.")
        return []

    # ---------------- WebSocket ----------------
    def _candidate_topics(self, symbol: str, tf: str) -> List[str]:
        s = str(tf).strip().lower()
        variants = [s]
        try:
            if s.isdigit():
                variants.append(f"{int(s)}m")
            elif s.endswith("m") and s[:-1].isdigit():
                variants.append(str(int(s[:-1])))
        except Exception:
            pass
        tops = []
        for iv in variants:
            tops.extend([
                f"kline.{iv}.{symbol}",
                f"klineV2.{iv}.{symbol}",
                f"candle.{iv}.{symbol}",
                f"public.kline.{iv}.{symbol}",
                f"instrument.kline.{iv}.{symbol}"
            ])
        seen = set()
        out = []
        for t in tops:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    async def start_kline_ws(self) -> None:
        # Respect USE_WS env also at runtime
        if not _env_true("USE_WS"):
            logger.info("start_kline_ws: USE_WS disabled by env; not starting websocket")
            return
        if self._ws_task and not self._ws_task.done():
            logger.debug("WS task already running")
            return
        self._ws_stop = False
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("Started Bybit WS task")

    async def stop_kline_ws(self) -> None:
        self._ws_stop = True
        if self._ws_task:
            try:
                self._ws_task.cancel()
            except Exception:
                logger.debug("Error cancelling ws task", exc_info=True)
            self._ws_task = None
        try:
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
        except Exception:
            logger.exception("Error closing ws connection")
        try:
            if self._ws_session:
                await self._ws_session.close()
        except Exception:
            logger.exception("Error closing ws session")
        self._ws = None
        self._ws_session = None
        while not self._pending_subscribe.empty():
            try:
                self._pending_subscribe.get_nowait()
            except Exception:
                break
        logger.info("Stopped Bybit WS")

    async def subscribe_klines_for_symbols(self, symbols: List[str], tfs: List[str]) -> None:
        if not symbols or not tfs:
            return
        for sym in symbols:
            for tf in tfs:
                key = (sym.upper(), tf)
                if key in self._requested_subs:
                    continue
                self._requested_subs.add(key)
                await self._pending_subscribe.put(("subscribe", sym.upper(), tf))
        logger.info("Queued subscribe requests for %d symbols x %d tfs", len(symbols), len(tfs))

    async def subscribe_mtf_for_symbol(self, symbol: str, mtfs: List[str]) -> None:
        symbol = symbol.upper()
        if not mtfs:
            return
        for tf in mtfs:
            key = (symbol, tf)
            if key in self._mtf_subscribed:
                continue
            self._mtf_subscribed.add(key)
            if key not in self._requested_subs:
                self._requested_subs.add(key)
                await self._pending_subscribe.put(("subscribe", symbol, tf))
        logger.info("Queued MTF subscribe for %s tfs=%s", symbol, mtfs)

    async def sub_kline(self, symbol: str, tf: str) -> bool:
        """
        Try to subscribe immediately using several candidate topic formats.
        If USE_WS disabled via env, do nothing and return False (quiet).
        """
        if not _env_true("USE_WS"):
            logger.debug("sub_kline: USE_WS disabled by env; skipping subscribe for %s %s", symbol, tf)
            return False

        symbol = symbol.upper()
        candidates = self._candidate_topics(symbol, tf)
        if self._ws and not self._ws.closed:
            for topic in candidates:
                try:
                    await self._ws.send_json({"op": "subscribe", "args": [topic]})
                    logger.debug("sub_kline: subscribed topic=%s for %s %s", topic, symbol, tf)
                    return True
                except Exception as e:
                    logger.debug("sub_kline: attempt failed for topic=%s (%s). err=%s", topic, symbol, tf, e)
        key = (symbol, tf)
        if key not in self._requested_subs:
            self._requested_subs.add(key)
            await self._pending_subscribe.put(("subscribe", symbol, tf))
        logger.debug("sub_kline: queued subscribe for %s %s (WS not connected or immediate attempts failed)", symbol, tf)
        return False

    async def _handle_ws_message(self, msg: Dict[str, Any]):
        # WS debug logging to console (Render logs)
        if _env_true("DEBUG_WS_LOG"):
            try:
                topic_dbg = msg.get("topic") or msg.get("arg") or msg.get("topicName") or "no-topic"
                try:
                    j = json.dumps(msg, default=str)
                except Exception:
                    j = str(msg)
                snippet = j if len(j) <= 2000 else (j[:2000] + " ...[truncated]")
                logger.info("_handle_ws_message: topic=%s snippet=%s", topic_dbg, snippet)
            except Exception:
                logger.exception("_handle_ws_message: debug logging failed")

        try:
            if msg.get("success") is not None and "request" in msg:
                return
            if "ping" in msg:
                try:
                    await self._ws.send_json({"pong": msg["ping"]})
                except Exception:
                    pass
                return
            if msg.get("type") in ("pong",):
                return

            topic = msg.get("topic") or msg.get("arg") or msg.get("topicName")
            data = msg.get("data")
            if not data and isinstance(msg.get("result"), dict) and "data" in msg["result"]:
                data = msg["result"]["data"]

            if topic and data:
                parts = str(topic).split(".")
                tf = None
                symbol = None
                if len(parts) >= 3:
                    tf = parts[-2] if len(parts) >= 3 else None
                    symbol = parts[-1]
                else:
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                        symbol = data[0].get("symbol") or data[0].get("s") or data[0].get("instrument_name")
                        tf = data[0].get("interval") or data[0].get("period") or data[0].get("klineInterval")
                if not symbol or not tf:
                    return
                symbol = str(symbol).upper()
                tf = str(tf)
                self._ensure_cache_slot(symbol, tf)
                seq = data if isinstance(data, (list, tuple)) else [data]
                for entry in seq:
                    norm = self._normalize_ws_kline_item(entry, tf)
                    if not norm:
                        continue
                    dq = self._kline_cache[symbol][tf]
                    if dq and len(dq) and dq[-1].get("start_at") == norm.get("start_at"):
                        dq[-1] = norm
                    else:
                        dq.append(norm)
                return
        except Exception:
            logger.exception("Error handling WS message: %s", traceback.format_exc())

    def _ensure_cache_slot(self, symbol: str, tf: str):
        symbol = symbol.upper()
        if tf not in self._kline_cache.get(symbol, {}):
            maxlen = max(100, int(KLINE_SEED_LIMIT) if isinstance(KLINE_SEED_LIMIT, int) and KLINE_SEED_LIMIT > 0 else 300)
            self._kline_cache.setdefault(symbol, {})[tf] = deque(maxlen=maxlen)

    def _normalize_ws_kline_item(self, raw: Dict[str, Any], tf: str) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        start = raw.get("start_at") or raw.get("start") or raw.get("t") or raw.get("open_time") or raw.get("timestamp")
        close = raw.get("close") or raw.get("close_price") or raw.get("c") or raw.get("last_price")
        vol = raw.get("volume") or raw.get("vol") or raw.get("v") or raw.get("turnover")
        is_closed = raw.get("is_closed") or raw.get("isClosed") or raw.get("isFinal") or raw.get("confirm")
        try:
            if start is not None:
                start = int(start)
        except Exception:
            start = None
        try:
            if close is not None:
                close = float(close)
        except Exception:
            close = None
        try:
            if vol is not None:
                vol = float(vol)
        except Exception:
            vol = None
        return {"start_at": start, "close": close, "volume": vol, "is_closed": is_closed}

    async def _ws_loop(self):
        while not self._ws_stop:
            connected = False
            for ws_url in self.ws_hosts:
                try:
                    if not self._ws_session:
                        self._ws_session = aiohttp.ClientSession()
                    logger.info("Attempting WS connect to %s", ws_url)
                    async with self._ws_session.ws_connect(ws_url, heartbeat=30) as ws:
                        self._ws = ws
                        self._ws_backoff = 1.0
                        connected = True
                        logger.info("Bybit WS connected to %s", ws_url)

                        pending = []
                        while not self._pending_subscribe.empty():
                            try:
                                pending.append(self._pending_subscribe.get_nowait())
                            except Exception:
                                break
                        for (sym, tf) in list(self._requested_subs):
                            pending.append(("subscribe", sym, tf))

                        for op, sym, tf in pending:
                            if op != "subscribe":
                                continue
                            for topic in self._candidate_topics(sym, tf):
                                try:
                                    await ws.send_json({"op": "subscribe", "args": [topic]})
                                    logger.debug("WS subscribe requested topic=%s", topic)
                                except Exception:
                                    logger.debug("Failed to send subscribe for %s", topic, exc_info=True)

                        async def ping_loop():
                            try:
                                while True:
                                    await asyncio.sleep(20)
                                    try:
                                        await ws.send_json({"op": "ping"})
                                    except Exception:
                                        break
                            except asyncio.CancelledError:
                                return

                        ping_task = asyncio.create_task(ping_loop())

                        async for raw in ws:
                            if raw.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    msg = json.loads(raw.data)
                                except Exception:
                                    logger.debug("Non-JSON WS message: %s", raw.data[:400])
                                    continue
                                await self._handle_ws_message(msg)
                            elif raw.type == aiohttp.WSMsgType.ERROR:
                                logger.error("WS error frame: %s", raw)
                                break
                            elif raw.type == aiohttp.WSMsgType.CLOSED:
                                logger.info("WS closed by server")
                                break

                        try:
                            ping_task.cancel()
                        except Exception:
                            pass

                except client_exceptions.WSServerHandshakeError as wh:
                    logger.warning("Bybit WS handshake failed for %s: %s", ws_url, getattr(wh, 'message', repr(wh)))
                    continue
                except asyncio.CancelledError:
                    logger.info("WS loop cancelled")
                    return
                except Exception:
                    logger.exception("Bybit WS connection error when connecting to %s", ws_url)
                    continue
                finally:
                    try:
                        if self._ws and not self._ws.closed:
                            await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

                if connected:
                    break

            if not connected:
                logger.error("Bybit WS: none of the WS endpoints accepted connection; backing off %.1fs before retrying", self._ws_backoff)
                await asyncio.sleep(self._ws_backoff)
                self._ws_backoff = min(self._ws_backoff * 2.0, 120.0)
                continue

        logger.info("Exiting WS loop")

    async def get_ws_klines(self, symbol: str, tf: str) -> List[Dict[str, Any]]:
        try:
            c = self._kline_cache.get(symbol.upper(), {}).get(tf, None)
            if c is None:
                return []
            return list(c)
        except Exception:
            return []

    def get_ws_latest_kline(self, symbol: str, tf: str) -> Optional[Dict[str, Any]]:
        try:
            c = self._kline_cache.get(symbol.upper(), {}).get(tf, None)
            if not c:
                return None
            return dict(c[-1])
        except Exception:
            return None

    def is_ws_connected(self) -> bool:
        try:
            return self._ws is not None and not self._ws.closed
        except Exception:
            return False
