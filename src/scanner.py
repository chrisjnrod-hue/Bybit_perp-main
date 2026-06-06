# Updated scanner.py
# Add this COMPLETE diagnostic version to replace your current scanner.py
# This version updates only MTF alignment & related behavior as requested.

import os
import asyncio
import time
import json
from collections import defaultdict
from typing import Dict, List, Any, Optional, Callable
from decimal import Decimal, ROUND_DOWN, getcontext
import math
import inspect

from .logger import get_logger
from .bybit_client import BybitClient
from .macd import macd_histogram, slope
from .config import (
    EXCLUDE_STABLECOINS, CONCURRENCY, KLINE_SEED_LIMIT,
    ROOT_TFS, MTF_TFS, ROOT_SCAN_INTERVAL, TRADE_ENABLED,
    MTF_SLOPE_LOOKBACK, ROOT_FILTER, ROOT_TOP_N, MTF_FILTER, MAX_OPEN_TRADES, USE_WS
)
from .telegram import send_message
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket

getcontext().prec = 28
logger = get_logger("scanner")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
REST_POLL_INTERVAL = int(os.getenv("REST_POLL_INTERVAL", "5"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "3"))
REQUEST_BATCH_SIZE = int(os.getenv("REQUEST_BATCH_SIZE", "5"))
REQUEST_BATCH_DELAY = float(os.getenv("REQUEST_BATCH_DELAY", "0.5"))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")

# Diagnostics
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# New: macd threshold for signal strength calculations
MACD_HIST_THRESHOLD = float(os.getenv("MACD_HIST_THRESHOLD", "0.0"))

logger.info("scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d MAX_CONCURRENT=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s)",
            bool(USE_WS), SEED_KLINES_LIMIT, MAX_CONCURRENT_REQUESTS, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE)


class Scanner:
    def __init__(self):
        self.rate_limiter = TokenBucket(max(1.0, float(1)))
        self.client = BybitClient(rate_limiter=self.rate_limiter)
        self.trade_manager = TradeManager()
        self.concurrent_sem = asyncio.Semaphore(max(1, CONCURRENCY))
        self.request_sem = asyncio.Semaphore(max(1, MAX_CONCURRENT_REQUESTS))
        self.kline_store: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(dict)
        self.symbols: List[str] = []
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._rest_poller_task: Optional[asyncio.Task] = None
        self._callbacks: List[Callable[[str, Any], Any]] = []
        self._24h_volumes: Dict[str, Dict[str, float]] = {}
        self._last_price_cache: Dict[str, float] = {}
        self._last_price_time: Dict[str, float] = {}
        # Deployment & pending opens: send signals on deploy but defer opening trades until next interval
        self._deployed = True
        self._pending_opens: List[Dict[str, Any]] = []
        logger.info("scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d MAX_CONCURRENT=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s)",
                   bool(USE_WS), SEED_KLINES_LIMIT, MAX_CONCURRENT_REQUESTS, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE)

    def register_callback(self, cb: Callable[[str, Any], Any]):
        if not callable(cb):
            raise TypeError("callback must be callable")
        self._callbacks.append(cb)

    async def _emit_event(self, event: str, payload: Any):
        for cb in list(self._callbacks):
            try:
                if inspect.iscoroutinefunction(cb):
                    await cb(event, payload)
                else:
                    res = cb(event, payload)
                    if inspect.isawaitable(res):
                        await res
            except Exception:
                logger.exception("Callback for event %s failed", event)

    async def _call_client_method(self, names: List[str], *args, **kwargs):
        for name in names:
            try:
                fn = getattr(self.client, name, None)
                if not fn:
                    continue
                res = fn(*args, **kwargs)
                if inspect.isawaitable(res):
                    res = await res
                return res
            except Exception:
                logger.debug("Client method %s failed", name, exc_info=True)
                continue
        logger.debug("No client method among %s succeeded", names)
        return None

    async def _get_symbols(self) -> List[str]:
        try:
            items = await self._call_client_method(["get_symbols", "getSymbols", "get_symbols", "symbols"])
        except Exception:
            logger.exception("Error fetching symbols from client")
            items = None

        if not items:
            logger.info("No symbols returned from client")
            await self._emit_event("symbols", [])
            self.symbols = []
            return []

        if isinstance(items, dict):
            if "data" in items and isinstance(items["data"], (list, dict)):
                items = items["data"]
            elif "result" in items and isinstance(items["result"], (list, dict)):
                items = items["result"]

        if isinstance(items, (str,)):
            items = [items]

        syms = []
        for it in items:
            try:
                if isinstance(it, str):
                    sym = it.strip().upper()
                    syms.append(sym)
                    continue
                if not isinstance(it, dict):
                    try:
                        v = str(it)
                        syms.append(v.upper())
                    except Exception:
                        continue
                    continue

                symbol = (
                    it.get("name")
                    or it.get("symbol")
                    or it.get("symbolName")
                    or it.get("instrument_name")
                    or it.get("instrument_id")
                    or it.get("id")
                )
                if not symbol:
                    base = it.get("baseCoin") or it.get("base")
                    quote = it.get("quoteCoin") or it.get("quote")
                    if base and quote:
                        symbol = f"{base}{quote}"

                if not symbol:
                    continue
                symbol = str(symbol).upper()

                expiry = (
                    it.get("expiry_time") or it.get("deliveryTime") or it.get("delivery_time")
                    or it.get("expiry") or it.get("expireTime") or it.get("delivery")
                )
                has_expiry = False
                if expiry is not None:
                    try:
                        if isinstance(expiry, (int, float)):
                            has_expiry = int(expiry) != 0
                        elif isinstance(expiry, str):
                            s = expiry.strip()
                            if s == "" or s in ("0", "0.0"):
                                has_expiry = False
                            else:
                                try:
                                    has_expiry = int(float(s)) != 0
                                except Exception:
                                    has_expiry = True
                        else:
                            has_expiry = True
                    except Exception:
                        has_expiry = True
                if has_expiry:
                    continue

                if not symbol.endswith("USDT"):
                    quote = it.get("quoteCoin") or it.get("quote")
                    if quote and str(quote).upper() != "USDT":
                        continue
                    inst_type = it.get("type") or it.get("instrumentType") or it.get("category") or it.get("contractType")
                    if inst_type and "PERP" not in str(inst_type).upper() and "PERPETUAL" not in str(inst_type).upper():
                        continue

                base = symbol.replace("USDT", "")
                if base in [s.upper() for s in EXCLUDE_STABLECOINS]:
                    continue

                syms.append(symbol)
            except Exception:
                logger.exception("Error normalizing symbol entry: %s", it)

        syms = sorted(set(syms))
        logger.info("Discovered %d USDT perpetual symbols", len(syms))
        await self._emit_event("symbols", syms)
        self.symbols = syms
        return syms

    async def discover_symbols(self) -> List[str]:
        try:
            logger.info("[DIAGNOSTIC] discover_symbols: STARTING")
            syms = await self._get_symbols()
            if not syms:
                logger.warning("[DIAGNOSTIC] discover_symbols: NO SYMBOLS FOUND!")
                return []

            logger.info("[DIAGNOSTIC] discover_symbols: Found %d symbols", len(syms))

            if USE_WS:
                try:
                    await self.client.start_kline_ws()
                except Exception:
                    logger.exception("Failed to start client WS")
            else:
                logger.info("USE_WS is False; websocket startup and subscriptions skipped (REST-only mode)")

            if USE_WS and syms:
                tasks = []
                sem = asyncio.Semaphore(max(1, CONCURRENCY))
                for sym in syms:
                    for tf in ROOT_TFS:
                        async def worker(s=sym, t=tf):
                            async with sem:
                                try:
                                    if hasattr(self.client, "sub_kline"):
                                        await self.client.sub_kline(s, t)
                                except Exception:
                                    logger.exception("sub_kline error for %s %s", s, t)
                        tasks.append(asyncio.create_task(worker()))
                if tasks:
                    await asyncio.gather(*tasks)

            await self._ensure_rest_poller()
            logger.info("[DIAGNOSTIC] discover_symbols: COMPLETE - ready to scan")
            return syms
        except Exception:
            logger.exception("discover_symbols failed")
            return []

    def _tf_to_seconds(self, tf: str) -> int:
        try:
            s = str(tf)
            if s.endswith("m"):
                return int(s[:-1]) * 60
            if s.endswith("h"):
                return int(s[:-1]) * 3600
            if s == "D" or s.endswith("d"):
                try:
                    if s == "D":
                        return 24 * 3600
                    return int(s[:-1]) * 86400
                except Exception:
                    return 24 * 3600
        except Exception:
            pass
        return 60

    async def _call_get_klines(self, symbol: str, tf: str, limit: int):
        names = ["get_klines", "getKlines", "get_klines_v2", "get_kline", "getKline"]
        return await self._call_client_method(names, symbol, tf, limit)

    def _normalize_klines(self, raw_klines: Any, tf: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if not raw_klines:
            return out

        if isinstance(raw_klines, dict):
            if "list" in raw_klines and isinstance(raw_klines["list"], (list, dict)):
                raw_klines = raw_klines["list"]
            elif "result" in raw_klines and isinstance(raw_klines["result"], (list, dict)):
                raw_klines = raw_klines["result"]
            elif "data" in raw_klines and isinstance(raw_klines["data"], (list, dict)):
                raw_klines = raw_klines["data"]

        seq = raw_klines if isinstance(raw_klines, (list, tuple)) else [raw_klines]
        for item in seq:
            try:
                if isinstance(item, (list, tuple)):
                    start = None
                    close = None
                    vol = None
                    if len(item) >= 1:
                        try:
                            start = int(item[0])
                        except Exception:
                            start = None
                    if len(item) >= 5:
                        try:
                            close = float(item[4])
                        except Exception:
                            close = None
                    if len(item) >= 6:
                        try:
                            vol = float(item[5])
                        except Exception:
                            vol = None

                    if close is not None:
                        out.append({"start_at": start, "close": close, "volume": vol})
                    continue

                if isinstance(item, dict):
                    start = (
                        item.get("start_at")
                        or item.get("open_time")
                        or item.get("t")
                        or item.get("timestamp")
                        or item.get("start")
                        or item.get("time")
                    )
                    close = (
                        item.get("close")
                        or item.get("close_price")
                        or item.get("c")
                        or item.get("last_price")
                        or item.get("Close")
                    )
                    vol = (
                        item.get("volume")
                        or item.get("vol")
                        or item.get("turnover")
                        or item.get("v")
                        or item.get("quoteAsset")
                    )
                    is_closed = item.get("isClosed")
                    if is_closed is None:
                        is_closed = item.get("is_closed")
                    if is_closed is None:
                        is_closed = item.get("complete")
                    if is_closed is None:
                        is_closed = item.get("confirmed")

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

                    if close is not None:
                        out.append({"start_at": start, "close": close, "volume": vol, "is_closed": is_closed})
                    continue

            except Exception:
                logger.exception("Failed to normalize kline item: %s", item)
                continue

        if out:
            try:
                last = out[-1]
                last_start = last.get("start_at")
                is_closed = last.get("is_closed", None)

                logger.debug(
                    "[CANDLE_STATUS] tf=%s start=%s is_closed=%s candles=%d",
                    tf,
                    last_start,
                    is_closed,
                    len(out)
                )
            except Exception:
                logger.exception("Error evaluating candle status")
        return out

    async def seed_klines_for_symbol(self, symbol: str):
        if SEED_KLINES_LIMIT < 100:
            logger.warning("SEED_KLINES_LIMIT is low (%d); consider >=200", SEED_KLINES_LIMIT)
        tfs = list(set(ROOT_TFS + MTF_TFS))
        for tf in tfs:
            try:
                logger.debug("seed_klines_for_symbol: requesting %s %s with limit=%d", symbol, tf, SEED_KLINES_LIMIT)

                async with self.request_sem:
                    raw = await self._call_get_klines(symbol, tf, limit=SEED_KLINES_LIMIT)

                if not raw:
                    logger.debug("No klines returned for %s %s (raw empty)", symbol, tf)
                    continue

                if DEBUG_SURGICAL_LOGS:
                    try:
                        if isinstance(raw, dict):
                            logger.info("[SURGICAL_LOG_0] API_KEYS %s %s - Response dict keys: %s", symbol, tf, list(raw.keys()))
                            for key in ["list", "result", "data"]:
                                if key in raw and isinstance(raw[key], (list, tuple)) and raw[key]:
                                    first_item = raw[key][0]
                                    logger.info("[SURGICAL_LOG_0] FIRST_ITEM %s %s - Key '%s' contains: type=%s, value=%s",
                                                symbol, tf, key, type(first_item).__name__, str(first_item)[:200])
                                    break
                        elif isinstance(raw, (list, tuple)):
                            logger.info("[SURGICAL_LOG_0] API_RESPONSE %s %s - Response is list/tuple, first item: type=%s, value=%s",
                                        symbol, tf, type(raw[0]).__name__ if raw else "empty", str(raw[0])[:200] if raw else "empty")
                    except Exception as e:
                        logger.info("[SURGICAL_LOG_0] API_RESPONSE %s %s - Failed to log structure: %s", symbol, tf, str(e)[:100])

                if DEBUG_SURGICAL_LOGS:
                    try:
                        if isinstance(raw, dict) and "list" in raw:
                            sample_raw = raw["list"][:3] if isinstance(raw["list"], list) else raw["list"]
                        elif isinstance(raw, dict) and "result" in raw:
                            sample_raw = raw["result"][:3] if isinstance(raw["result"], list) else raw["result"]
                        elif isinstance(raw, dict) and "data" in raw:
                            sample_raw = raw["data"][:3] if isinstance(raw["data"], list) else raw["data"]
                        elif isinstance(raw, list):
                            sample_raw = raw[:3]
                        else:
                            sample_raw = str(raw)[:200]
                        logger.info("[SURGICAL_LOG_1] RAW_RESPONSE %s %s: type=%s, sample=%s", symbol, tf, type(raw).__name__, sample_raw)
                    except Exception as e:
                        logger.info("[SURGICAL_LOG_1] RAW_RESPONSE %s %s: failed to log - %s", symbol, tf, str(e)[:100])

                normalized = self._normalize_klines(raw, tf)

                valid = []
                for c in normalized:
                    try:
                        if not isinstance(c, dict):
                            continue
                        close = c.get("close")
                        start = c.get("start_at")
                        if close is None:
                            continue
                        if isinstance(close, (int, float)) and math.isfinite(float(close)):
                            valid.append({"start_at": start, "close": float(close), "volume": c.get("volume")})
                    except Exception:
                        continue

                if DEBUG_SURGICAL_LOGS:
                    logger.info("[SURGICAL_LOG_2] NORMALIZE %s %s: raw_count=%d, normalized_count=%d, valid_count=%d",
                                symbol, tf, len(raw) if isinstance(raw, (list, tuple)) else 1, len(normalized), len(valid))
                    if len(valid) == 0 and len(normalized) > 0:
                        sample_norm = normalized[:2]
                        logger.warning("[SURGICAL_LOG_2] FILTERED_OUT: first 2 normalized items: %s", sample_norm)

                if not valid:
                    try:
                        txt = json.dumps(raw, default=str)
                    except Exception:
                        txt = str(raw)
                    snippet_trunc = (txt[:500] + '...') if len(txt) > 500 else txt
                    logger.debug("Seeded 0 usable candles for %s %s. Raw response (truncated): %s", symbol, tf, snippet_trunc)
                    continue

                try:
                    klines_sorted = sorted(valid, key=lambda x: x.get("start_at") or 0)
                except Exception:
                    klines_sorted = valid
                self.kline_store[symbol][tf] = klines_sorted
                logger.debug("Seeded %s %s candles=%d", symbol, tf, len(klines_sorted))
                await self._emit_event("klines_seeded", {"symbol": symbol, "tf": tf, "count": len(klines_sorted)})
            except Exception:
                logger.exception("Seed klines failed for %s %s", symbol, tf)

    async def seed_all(self):
        logger.info("[DIAGNOSTIC] seed_all: STARTING with %d symbols", len(self.symbols))
        async def worker(sym: str):
            async with self.concurrent_sem:
                await self.seed_klines_for_symbol(sym)

        for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
            batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
            tasks = [asyncio.create_task(worker(s)) for s in batch]
            if tasks:
                await asyncio.gather(*tasks)
            if i + REQUEST_BATCH_SIZE < len(self.symbols):
                await asyncio.sleep(REQUEST_BATCH_DELAY)

        logger.info("[DIAGNOSTIC] seed_all: COMPLETE")

    async def _rest_poller(self):
        logger.info("REST poller started (interval=%s seconds)", REST_POLL_INTERVAL)
        poll_count = 0
        try:
            while not self._stop and (not USE_WS or not self.client.is_ws_connected()):
                poll_count += 1
                if poll_count % 5 == 0:
                    logger.info("[REST_POLLER] Active poll #%d, symbols=%d", poll_count, len(self.symbols))

                start = time.time()
                if not self.symbols:
                    await asyncio.sleep(REST_POLL_INTERVAL)
                    continue

                async def poll_symbol(sym: str):
                    async with self.request_sem:
                        for root in ROOT_TFS:
                            try:
                                data = await self.client.get_klines(sym, root, limit=3)
                                normalized = self._normalize_klines(data, root) if data else []
                                if normalized:
                                    lst = self.kline_store.get(sym, {}).get(root, [])
                                    last_new = None
                                    for c in reversed(normalized):
                                        if c.get("close") is not None:
                                            last_new = {"start_at": c.get("start_at"), "close": float(c.get("close")), "volume": c.get("volume")}
                                            break
                                    if last_new:
                                        if lst:
                                            try:
                                                if lst[-1].get("start_at") == last_new.get("start_at"):
                                                    lst[-1] = last_new
                                                else:
                                                    lst.append(last_new)
                                            except Exception:
                                                self.kline_store.setdefault(sym, {})[root] = [last_new]
                                        else:
                                            self.kline_store.setdefault(sym, {})[root] = [last_new]
                            except Exception:
                                logger.debug("REST poll kline failed for %s %s", sym, root, exc_info=True)

                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(poll_symbol(s)) for s in batch]
                    try:
                        await asyncio.wait(tasks, timeout=REST_POLL_INTERVAL)
                    except Exception:
                        pass

                elapsed = time.time() - start
                to_sleep = max(0, REST_POLL_INTERVAL - elapsed)
                if USE_WS and self.client.is_ws_connected():
                    logger.info("WS reconnected; stopping REST poller")
                    break
                await asyncio.sleep(to_sleep)
        except asyncio.CancelledError:
            logger.info("REST poller cancelled")
        except Exception:
            logger.exception("REST poller encountered an exception")
        logger.info("REST poller stopped")

    async def _ensure_rest_poller(self):
        if USE_WS and self.client.is_ws_connected():
            if self._rest_poller_task and not self._rest_poller_task.done():
                try:
                    self._rest_poller_task.cancel()
                except Exception:
                    pass
                self._rest_poller_task = None
            return
        if self._rest_poller_task and not self._rest_poller_task.done():
            return
        self._rest_poller_task = asyncio.create_task(self._rest_poller())

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        data = self.kline_store.get(symbol, {}).get(tf, [])
        closes: List[float] = []
        for c in data:
            try:
                if isinstance(c, dict) and c.get("close") is not None:
                    closes.append(float(c.get("close")))
                elif isinstance(c, (int, float)):
                    closes.append(float(c))
            except Exception:
                continue
        # Replace current candle close instead of appending fake candle
        if include_price is not None:
            if closes:
                closes[-1] = float(include_price)
            else:
                closes.append(float(include_price))
        elif use_ws_current and USE_WS:
            try:
                ws_last = self.client.get_ws_latest_kline(symbol, tf) if hasattr(self.client, "get_ws_latest_kline") else None
                if ws_last and ws_last.get("close") is not None:
                    closes = closes + [float(ws_last.get("close"))]
            except Exception:
                pass

        macd_line, signal_line, hist = macd_histogram(closes)
        if DEBUG_SURGICAL_LOGS:
            valid_hist_count = sum(1 for h in hist if h is not None) if hist else 0
            logger.info("[SURGICAL_LOG_3] MACD_CALC %s %s: closes_count=%d, hist_length=%d, valid_hist=%d, last_hist=%s",
                        symbol, tf, len(closes), len(hist) if hist else 0, valid_hist_count, hist[-1] if hist and len(hist) > 0 else None)

        if DEBUG_SURGICAL_LOGS and len(closes) > 0:
            try:
                last_10_hist = hist[-10:] if hist and len(hist) >= 10 else (hist if hist else [])
                logger.info("[MACD_DEBUG] %s %s: closes=%d, hist_last_10=%s",
                           symbol, tf, len(closes), last_10_hist)
            except Exception as e:
                logger.info("[MACD_DEBUG] %s %s: error formatting histogram: %s", symbol, tf, str(e)[:50])

        try:
            hist = [None if v is None else float(v) for v in (hist or [])]
        except Exception:
            pass
        return macd_line, signal_line, hist

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        if not hist or len(hist) < 2:
            if DEBUG_SURGICAL_LOGS and (symbol or tf):
                logger.info("[SURGICAL_LOG_4] FLIP_CHECK %s %s: insufficient_hist (len=%d)", symbol, tf, len(hist) if hist else 0)
            return False
        prev = hist[-2]
        cur = hist[-1]
        if prev is None or cur is None:
            if DEBUG_SURGICAL_LOGS and (symbol or tf):
                logger.info("[SURGICAL_LOG_4] FLIP_CHECK %s %s: None_values (prev=%s, cur=%s)", symbol, tf, prev, cur)
            return False
        try:
            zero_cross = prev <= 0 and cur > 0
            hist_change = cur - prev
            strong_flip = True
            result = zero_cross

            if DEBUG_SURGICAL_LOGS:
                logger.info("[FLIP_DEBUG] %s %s: prev=%.8f, cur=%.8f, change=%.8f, zero_cross=%s, strong=%s, FLIP=%s",
                           symbol, tf, prev, cur, hist_change, zero_cross, strong_flip, result)

            if DEBUG_SURGICAL_LOGS and (symbol or tf):
                logger.info("[SURGICAL_LOG_4] FLIP_CHECK %s %s: prev=%.6f, cur=%.6f, threshold=%s, flip=%s",
                           symbol, tf, prev, cur, hist_threshold, result)

            if result and DEBUG_SURGICAL_LOGS:
                logger.warning("[FLIP_DETECTED_INTERNAL] %s %s: STRONG FLIP! prev=%.8f Ã¢â€ â€™ cur=%.8f (change=%.8f)",
                              symbol, tf, prev, cur, hist_change)

            return result
        except Exception:
            logger.exception("Error comparing hist values %s %s", prev, cur)
            return False

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        if qty is None:
            return 0.0
        qty_d = Decimal(str(qty))
        if step is None or step <= 0:
            if min_qty and qty_d < Decimal(str(min_qty)):
                logger.debug("Qty below min_qty, bumping to min_qty %s", min_qty)
                return float(Decimal(str(min_qty)))
            return float(qty_d)
        step_d = Decimal(str(step))
        mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
        quant = (mult * step_d)
        if min_qty is not None:
            min_d = Decimal(str(min_qty))
            if quant < min_d:
                logger.debug("Quantized qty %s below min_qty %s, using min_qty", float(quant), float(min_d))
                quant = min_d
        try:
            quant = quant.normalize()
        except Exception:
            pass
        return float(quant)

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        """Update and track 24h volume data with caching"""
        try:
            now = time.time()
            if symbol in self._last_price_time and (now - self._last_price_time[symbol]) < 60:
                return None

            if hasattr(self.client, "get_24h_ticker"):
                async with self.request_sem:
                    data = await self.client.get_24h_ticker(symbol)
                if data and isinstance(data, dict):
                    vol = data.get("volume") or data.get("vol") or data.get("turnover")
                    if vol is not None:
                        try:
                            vol = float(vol)
                            if symbol not in self._24h_volumes:
                                self._24h_volumes[symbol] = {"current": vol, "previous": vol}
                            else:
                                self._24h_volumes[symbol]["previous"] = self._24h_volumes[symbol]["current"]
                                self._24h_volumes[symbol]["current"] = vol
                            self._last_price_time[symbol] = now
                            return vol
                        except Exception:
                            pass
        except Exception:
            logger.debug("Could not update 24h volume for %s", symbol, exc_info=True)
        return None

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        """Compute percentage change in 24h volume"""
        try:
            if symbol not in self._24h_volumes:
                return None
            vol_data = self._24h_volumes[symbol]
            prev_vol = vol_data.get("previous", 0)
            curr_vol = vol_data.get("current", 0)
            if prev_vol <= 0:
                return None
            change = (curr_vol - prev_vol) / prev_vol
            return min(change, 1.0)
        except Exception:
            logger.debug("Could not compute 24h volume change for %s", symbol)
            return None

    async def _maybe_close_least_profitable(self):
        """
        Automatically close least profitable trade if we are within 5 minutes before the next round hour
        and open trades count >= MAX_OPEN_TRADES.
        This tries some common trade_manager methods; if none exist, it will attempt basic heuristics.
        """
        try:
            now = time.time()
            next_hour = math.ceil(now / 3600.0) * 3600.0
            secs_to_next_hour = next_hour - now
            # Trigger in the 5-minute window before the hour
            if 0 < secs_to_next_hour <= 300:
                current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
                if current_open >= MAX_OPEN_TRADES:
                    logger.info("Within 5m of hour and max open trades reached (%d/%d). Attempting to close least profitable trade.",
                                current_open, MAX_OPEN_TRADES)
                    # Prefer a built-in helper if present
                    if hasattr(self.trade_manager, "close_least_profitable"):
                        try:
                            await maybe_awaitable(self.trade_manager.close_least_profitable)
                            logger.info("Called trade_manager.close_least_profitable()")
                            return
                        except Exception:
                            logger.exception("trade_manager.close_least_profitable failed")
                    # Fallback heuristics: look for 'pnl'/'unrealized_pnl'/'profit' on open_trades
                    try:
                        open_trades = getattr(self.trade_manager, "open_trades", []) or []
                        if not open_trades:
                            logger.info("No open trades found to close")
                            return
                        # Try to pick least profitable by common keys
                        def profit_value(t):
                            for k in ("unrealized_pnl", "pnl", "profit", "unrealized", "floating"):
                                v = t.get(k) if isinstance(t, dict) else None
                                if v is not None:
                                    try:
                                        return float(v)
                                    except Exception:
                                        continue
                            # if no pnl info, fallback to 0
                            return 0.0
                        worst = min(open_trades, key=profit_value)
                        # attempt several possible method names to close the trade
                        closed = False
                        for name in ("close_trade", "close_position", "close_open_trade", "close_by_id"):
                            fn = getattr(self.trade_manager, name, None)
                            if fn:
                                try:
                                    args = ()
                                    # best effort: pass symbol or trade id if available
                                    if "symbol" in worst:
                                        args = (worst.get("symbol"),)
                                    elif "id" in worst:
                                        args = (worst.get("id"),)
                                    res = fn(*args)
                                    if inspect.isawaitable(res):
                                        await res
                                    logger.info("Called trade_manager.%s(%s) to close least profitable", name, args)
                                    closed = True
                                    break
                                except Exception:
                                    logger.exception("trade_manager.%s failed", name)
                        if not closed:
                            logger.warning("Could not programmatically close least profitable trade; no supported method found.")
                    except Exception:
                        logger.exception("Error while attempting fallback close of least profitable trade")
        except Exception:
            logger.exception("Error in _maybe_close_least_profitable")

    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1

            logger.warning(
                "[DIAGNOSTIC_SCAN_START] ============ ROOT SCAN START (cycle #%d) ============",
                loop_count
            )

            logger.info("[DIAGNOSTIC] root_scan_loop: Beginning scan cycle #%d", loop_count)

            start = time.time()
            try:
                # Ensure symbols present
                if not self.symbols:
                    logger.info("[DIAGNOSTIC] root_scan_loop: No symbols, discovering...")
                    await self.discover_symbols()
                    if self.symbols:
                        logger.info("[DIAGNOSTIC] root_scan_loop: Starting symbol seed (count=%d)", len(self.symbols))
                        await self.seed_all()
                        logger.info("[DIAGNOSTIC] root_scan_loop: Symbol seeding complete")
                    else:
                        logger.warning("[DIAGNOSTIC] root_scan_loop: Symbol discovery returned empty!")
                        await asyncio.sleep(10)
                        continue

                await self._ensure_rest_poller()

                # automatic close of least profitable trade 5min before the hour
                await self._maybe_close_least_profitable()

                root_signals: List[Dict[str, Any]] = []
                logger.info("[DIAGNOSTIC] root_scan_loop: Starting symbol checks (total=%d)", len(self.symbols))

                async def check_symbol(sym: str):
                    try:
                        async with self.request_sem:
                            price = await self.client.get_latest_price(sym)

                        if price is None:
                            try:
                                if USE_WS and self.client.is_ws_connected():
                                    ws_last = self.client.get_ws_latest_kline(sym, ROOT_TFS[0]) if hasattr(self.client, "get_ws_latest_kline") else None
                                    if ws_last and ws_last.get("close") is not None:
                                        price = float(ws_last.get("close"))
                            except Exception:
                                price = None

                        if price is None:
                            return

                        self._last_price_cache[sym] = price
                        await self._update_24h_volume(sym)

                        for root in ROOT_TFS:

                            logger.info(
                                "[ROOT_SCAN_CALC] %s %s: STARTING MACD calculation",
                                sym,
                                root
                            )

                            macd_line, sig, hist = self.compute_macd_for(
                                sym,
                                root,
                                include_price=price,
                                use_ws_current=True
                            )

                            logger.info(
                                "[ROOT_SCAN_CALC] %s %s: MACD calc complete, hist_len=%d, last_val=%s",
                                sym,
                                root,
                                len(hist) if hist else 0,
                                hist[-1] if hist and len(hist) > 0 else None
                            )

                            flip = self.detect_flip_current_open(
                                hist,
                                0.0,
                                symbol=sym,
                                tf=root
                            )

                            try:
                                prev_hist = hist[-2] if hist and len(hist) >= 2 else None
                                cur_hist = hist[-1] if hist and len(hist) >= 1 else None

                                logger.info(
                                    "[DEBUG-FLIP] %s %s prev=%s cur=%s flip=%s",
                                    sym,
                                    root,
                                    prev_hist,
                                    cur_hist,
                                    flip
                                )

                            except Exception:
                                logger.exception("DEBUG FLIP LOG FAILED")

                            logger.info(
                                "[ROOT_SCAN_RESULT] %s %s: flip_detected=%s",
                                sym,
                                root,
                                flip
                            )

                            if DEBUG_SURGICAL_LOGS:
                                logger.info("[ROOT_SCAN_CHECK] %s %s: hist_valid=%s, flip=%s",
                                           sym, root, hist is not None and len(hist) > 0, flip)

                            if hist and (cur_hist is not None):
                                vol_change = self.compute_24h_volume_change(sym)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "cur_hist": cur_hist,
                                    "prev_hist": prev_hist,
                                    "flip": flip,
                                    "vol_change": vol_change
                                })
                                if DEBUG_SURGICAL_LOGS:
                                    logger.warning("[SIGNAL_DETECTED_INTERNAL] %s %s price=%s cur_hist=%s flip=%s", sym, root, price, cur_hist, flip)

                    except Exception:
                        logger.exception("Error checking symbol %s", sym)

                checked_count = 0
                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(check_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks)
                    checked_count += len(batch)
                    if i + REQUEST_BATCH_SIZE < len(self.symbols):
                        await asyncio.sleep(REQUEST_BATCH_DELAY)

                logger.warning(
                    "[DIAGNOSTIC_SCAN_END] ============ ROOT SCAN COMPLETE (cycle #%d) ============",
                    loop_count
                )

                logger.warning(
                    "[SCAN_RESULTS] Checked=%d symbols, Signals=%d, ROOT_TFS=%s",
                    checked_count,
                    len(root_signals),
                    ROOT_TFS
                )

                logger.info("[DIAGNOSTIC] root_scan_loop: Checked %d symbols, found %d signals", checked_count, len(root_signals))
                logger.info("Root scan checked %d symbols, found %d signals", checked_count, len(root_signals))
                await self._emit_event("root_signals", root_signals)

                # If any signals discovered, subscribe MTF if supported
                if root_signals:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))

                # Evaluate and handle signals with new MTF alignment logic
                if root_signals:
                    await self.handle_root_signals(root_signals)
                else:
                    logger.info("No root signals this interval.")

                # Summary/telegram formatting per requested blocks
                await self.send_summary(root_signals)

                try:
                    candidates_count = len(root_signals) if root_signals else 0
                    logger.info("Ã¢Å“â€œ ROOT_SCAN_COMPLETE: checked=%d, signals=%d, candidates=%d",
                               checked_count, len(root_signals), candidates_count)
                except Exception:
                    pass

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start

            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                # If interval not set or falsy, run at open of every 5m TF candle
                now = time.time()
                next_5m = math.ceil(now / 300.0) * 300.0
                to_sleep = max(0, next_5m - now)
                logger.debug("ROOT_SCAN_INTERVAL not set; sleeping until next 5m open in %.1fs", to_sleep)
                await asyncio.sleep(to_sleep)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]):
        """
        New MTF alignment logic:
        - For each root signal, scan 5m,15m,1h,4h,1d TF for macd hist. Accept if any TF is positive or any TF has a current flip.
        - If 1 or more TFs are negative, monitor (do not open) until they flip on the current candle open.
        - Exception: If only 1d is negative but trending upwards (positive slope) accept/open.
        - Removed ROOT_FILTER and MTF_FILTER behaviors.
        - If self._deployed is True and TRADE_ENABLED True, queue opens and do not execute until next interval.
        """
        evaluated = []

        # If there were pending opens from previous cycle (deferred due to deploy), attempt to open them first
        if self._pending_opens:
            logger.info("Processing %d pending opens from previous cycle", len(self._pending_opens))
            # Try to open them now
            for p in list(self._pending_opens):
                try:
                    await self._attempt_open_candidate(p)
                except Exception:
                    logger.exception("Failed to open pending candidate %s", p.get("symbol"))
            self._pending_opens = []

        # per-signal MTF evaluation
        for item in root_signals:
            sym = item["symbol"]
            price = item["price"]
            root = item["root"]
            vol_change = item.get("vol_change")
            mtf_state = {}
            positive_count = 0
            flips_any = False
            negative_count = 0

            # Evaluate each MTF timeframe
            # Ensure MTF scan order as requested (5m,15m,1h,4h,1d)
            desired_mtfs = ["5m", "15m", "1h", "4h", "1d"]
            for tf in desired_mtfs:
                macd_line, sig, h = self.compute_macd_for(sym, tf, include_price=price, use_ws_current=True)
                cur_hist = h[-1] if h and len(h) >= 1 else None
                prev_hist = h[-2] if h and len(h) >= 2 else None
                mtf_state[tf] = {"prev": prev_hist, "cur": cur_hist}
                if cur_hist is not None and cur_hist > 0:
                    positive_count += 1
                if prev_hist is not None and prev_hist < 0 and cur_hist is not None and cur_hist > 0:
                    flips_any = True
                if cur_hist is not None and cur_hist < 0:
                    negative_count += 1

            # Compute one-d slope for exception C
            one_d_slope = None
            one_d_hist = None
            try:
                _, _, one_d_hist = self.compute_macd_for(sym, "1d", include_price=price, use_ws_current=True)
                if one_d_hist:
                    one_d_slope = slope(one_d_hist or [], lookback=MTF_SLOPE_LOOKBACK) if one_d_hist else None
            except Exception:
                logger.exception("Failed computing 1d slope for %s", sym)

            # Score: positive_count + flips + vol_change (bounded)
            score = float(positive_count)
            if flips_any:
                score += 1.0
            if vol_change is not None and vol_change > 0:
                score += min(vol_change, 1.0)

            # Decision logic per requested scenarios:
            accept = False
            reason = None

            if positive_count > 0 or flips_any:
                # If there are negative TFs, we should wait until they flip unless exception C applies
                if negative_count == 0:
                    accept = True
                    reason = "positive_alignment"
                else:
                    # Exception: only 1d is negative but it is rising -> accept per C
                    negative_tfs = [tf for tf, vals in mtf_state.items() if vals.get("cur") is not None and vals.get("cur") < 0]
                    if len(negative_tfs) == 1 and negative_tfs[0] == "1d" and one_d_slope is not None and one_d_slope > 0:
                        accept = True
                        reason = "one_d_negative_but_rising"
                    else:
                        accept = False
                        reason = "monitor_wait_for_flip"
            else:
                # no positives or flips detected -> monitor
                accept = False
                reason = "no_positive_mtf"

            evaluated.append({
                "symbol": sym,
                "root": root,
                "price": price,
                "mtf": mtf_state,
                "positive_count": positive_count,
                "flips_any": flips_any,
                "negative_count": negative_count,
                "vol_change": vol_change,
                "one_d_slope": one_d_slope,
                "accept": accept,
                "reason": reason,
                "score": score
            })

        await self._emit_event("candidates_evaluated", evaluated)

        # Open accepted candidates (respecting MAX_OPEN_TRADES and deployment deferral)
        # Order by score desc then positive_count
        candidates = sorted([e for e in evaluated if e["accept"]], key=lambda r: (r["score"], r["positive_count"]), reverse=True)

        logger.info("Opening candidates count=%d (MAX_OPEN_TRADES=%d)", len(candidates), MAX_OPEN_TRADES)
        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Reached max open trades; stopping opens.")
                break
            # If deployed and TRADE_ENABLED, defer opening until next scan interval
            if TRADE_ENABLED and self._deployed:
                logger.info("Deferring opens due to deployment: queuing candidate %s to open on next interval", c["symbol"])
                self._pending_opens.append(c)
                # mark that we've deferred so next cycles will execute
                self._deployed = False
                await send_message(f"Deployment: queued open for {c['symbol']} @ {c['price']} score={c['score']:.2f} (will open next interval)")
                continue

            # otherwise attempt to open (simulated vs live)
            try:
                sym = c["symbol"]
                price = c["price"]
                try:
                    balance = await self.client.get_balance("USDT")
                except Exception:
                    balance = None
                symbol_info = await self.client.get_symbol_info(sym)
                qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
                qty = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
                if qty <= 0 or math.isclose(qty, 0.0):
                    logger.warning("Computed qty for %s was zero after quantize (qty=%s). Skipping open.", sym, qty)
                    continue
                if qty != qty_raw:
                    logger.debug("Qty for %s adjusted from %s to %s (step=%s min=%s)", sym, qty_raw, qty, symbol_info.get("step"), symbol_info.get("min_qty"))
                side = "Buy"
                if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                    try:
                        order = await self.client.create_order(sym, side, qty)
                        self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                        await send_message(f"Opened trade {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f}")
                    except Exception:
                        logger.exception("Failed to place order for %s", sym)
                else:
                    t = self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"]})
                    logger.info("Simulated open %s qty=%s score=%.2f", sym, qty, c["score"])
                    await send_message(f"Simulated open {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f} reason={c['reason']}")
            except Exception:
                logger.exception("Error opening candidate %s", c.get("symbol"))

    async def _attempt_open_candidate(self, c: Dict[str, Any]):
        """Helper to open a candidate dict (used for pending opens)"""
        try:
            if not self.trade_manager.can_open():
                logger.info("Cannot open pending candidate %s; no capacity", c.get("symbol"))
                return
            sym = c["symbol"]
            price = c["price"]
            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None
            symbol_info = await self.client.get_symbol_info(sym)
            qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
            qty = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
            if qty <= 0 or math.isclose(qty, 0.0):
                logger.warning("Computed qty for %s was zero after quantize (qty=%s). Skipping open.", sym, qty)
                return
            side = "Buy"
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                order = await self.client.create_order(sym, side, qty)
                self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                await send_message(f"Opened trade {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f}")
            else:
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c['score']})
                await send_message(f"Simulated open {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f} reason={c['reason']}")
        except Exception:
            logger.exception("Failed to open candidate %s", c.get("symbol"))

    async def send_summary(self, root_signals: List[Dict[str, Any]]):
        """
        Telegram push notice per scan interval:
        - first block: root tfs signal summary in one block
        - subsequent blocks: for each root TF (1h,4h,1d in that order) list signals,
          each symbol with: title (Bybit Perp), symbol, price, signal strength (MACD_HIST_THRESHOLD + 24h vol change),
          MTF alignment state
        """
        try:
            if not root_signals:
                await send_message("Root scan: no signals this interval.")
                return

            # Build mapping by root timeframe
            grouped = {}
            for it in root_signals:
                grouped.setdefault(it["root"], []).append(it)

            lines = []
            # Summary block
            lines.append(f"Root scan summary ({len(root_signals)} signals)")
            for rt in ROOT_TFS:
                lst = grouped.get(rt, [])
                lines.append(f"- Root TF {rt}: {len(lst)} signal(s)")

            # Next blocks: prioritized order: 1h,4h,1d if those exist in ROOT_TFS; else use ROOT_TFS order reversed
            preferred_order = []
            if "1h" in ROOT_TFS or "60" in ROOT_TFS:
                preferred_order.append(next((r for r in ROOT_TFS if r in ("1h", "60")), None))
            if "4h" in ROOT_TFS or "240" in ROOT_TFS:
                preferred_order.append(next((r for r in ROOT_TFS if r in ("4h", "240")), None))
            if "1d" in ROOT_TFS or "D" in ROOT_TFS or "1d" in ROOT_TFS:
                preferred_order.append(next((r for r in ROOT_TFS if r in ("1d", "D")), None))
            # remove None and duplicates, fall back to ROOT_TFS order
            preferred_order = [p for p in preferred_order if p]
            if not preferred_order:
                preferred_order = list(ROOT_TFS)

            # For each root timeframe in the preferred order, create blocks
            for rt in preferred_order:
                lst = grouped.get(rt, [])
                if not lst:
                    continue
                lines.append("\n" + f"Root {rt} signals:")
                # For each signal in that root timeframe, detail a block
                # We want to list 1h signals first, then 4h, then 1d per user spec; done by preferred_order
                for s in lst:
                    sym = s["symbol"]
                    price = s["price"]
                    vol_change = s.get("vol_change")
                    # Signal strength heuristic: MACD_HIST_THRESHOLD + vol_change (if available)
                    strength = MACD_HIST_THRESHOLD + (vol_change if vol_change is not None else 0.0)
                    # MTF alignment state: build a short textual summary from computed mtf when possible
                    mtf_state = s.get("mtf")
                    if not mtf_state:
                        # If not present in root_signals (older entries), try to recompute small summary
                        mtf_summary = "mtf=N/A"
                    else:
                        parts = []
                        for tf, vals in (mtf_state.items() if isinstance(mtf_state, dict) else []):
                            cur = vals.get("cur")
                            prev = vals.get("prev")
                            state = "?"
                            if cur is None:
                                state = "n/a"
                            elif cur > 0:
                                state = "POS"
                            elif cur < 0:
                                state = "NEG"
                            if prev is not None and prev < 0 and cur is not None and cur > 0:
                                state += "/FLIP"
                            parts.append(f"{tf}:{state}")
                        mtf_summary = ", ".join(parts)
                    title = "Bybit Perp"
                    vol_str = f"{vol_change:.2f}" if vol_change is not None else "n/a"
                    lines.append(f"- {title} | {sym} | price={price} | strength={strength:.4f} | volΔ={vol_str} | mtf=({mtf_summary})")

            # Open trades summary (if any)
            open_sum = None
            try:
                open_sum = self.trade_manager.summary()
            except Exception:
                try:
                    # fallback: inspect trade_manager.open_trades
                    ot = getattr(self.trade_manager, "open_trades", None)
                    if ot:
                        open_sum = [{"symbol": t.get("symbol"), "qty": t.get("qty"), "entry": t.get("entry")} for t in ot]
                except Exception:
                    open_sum = None

            if open_sum:
                lines.append("\nOpen trades:")
                for ot in open_sum:
                    lines.append(f"- {ot.get('symbol')} {ot.get('qty')} @ {ot.get('entry')}")

            text = "\n".join(lines)
            await send_message(text)
        except Exception:
            logger.exception("Failed to send summary")

    async def run(self):
        # When run starts, we already treat the scanner as deployed (self._deployed True by default)
        # The first root scan will send signals but defer trade opens (if TRADE_ENABLED) until next cycle.
        self._task = asyncio.create_task(self.root_scan_loop())
        try:
            await self._task
        except asyncio.CancelledError:
            logger.info("Scanner run cancelled")
        finally:
            try:
                await self.client.close()
            except Exception:
                logger.exception("Error closing client")

    def stop(self):
        logger.info("Stopping scanner...")
        self._stop = True
        if self._task and not self._task.done():
            self._task.cancel()
        if self._rest_poller_task and not self._rest_poller_task.done():
            try:
                self._rest_poller_task.cancel()
            except Exception:
                pass


# Helper to await or call sync function
async def maybe_awaitable(fn_or_coro, *args, **kwargs):
    """
    Accepts either a callable or coroutine. If callable, calls it with args; if coroutine, awaits it.
    Returns the result.
    """
    try:
        if inspect.iscoroutinefunction(fn_or_coro):
            return await fn_or_coro(*args, **kwargs)
        res = fn_or_coro(*args, **kwargs)
        if inspect.isawaitable(res):
            return await res
        return res
    except Exception:
        raise
