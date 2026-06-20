# scanner.py - CORRECTED & VERIFIED - All MTF alignment + performance fixes applied
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
    MTF_SLOPE_LOOKBACK, ROOT_FILTER, ROOT_TOP_N, MAX_OPEN_TRADES, USE_WS,
    MAX_CONCURRENT_REQUESTS, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY,
    REST_POLL_INTERVAL
)
from .telegram import send_message
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket

getcontext().prec = 28
logger = get_logger("scanner")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# ============ FIX #1: MTF Alignment TFs — NUMERIC format matching Bybit API ============
MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]


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
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        self._symbol_check_count = 0
        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s)",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE
        )

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

            # FIX #3: Subscribe to correct TF format (numeric, not "5m" format)
            if USE_WS and syms:
                tasks = []
                sem = asyncio.Semaphore(max(1, CONCURRENCY))
                tfs_to_sub = list(set(list(ROOT_TFS) + ["5", "15"]))  # CORRECT: "5" not "5m"
                
                for sym in syms:
                    for tf in tfs_to_sub:
                        async def worker(s=sym, t=tf):
                            async with sem:
                                try:
                                    if hasattr(self.client, "sub_kline"):
                                        await self.client.sub_kline(s, t)
                                        logger.debug("[WS_SUB] Successfully subscribed to %s %s", s, t)
                                except Exception:
                                    logger.exception("sub_kline error for %s %s", s, t)
                        tasks.append(asyncio.create_task(worker()))
                
                # Process subscriptions in batches
                for i in range(0, len(tasks), 50):
                    batch = tasks[i:i+50]
                    await asyncio.gather(*batch)
                    await asyncio.sleep(0.1)
                
                logger.info("[DIAGNOSTIC] WS subscriptions complete for %d timeframes", len(tasks))

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
            # Handle numeric format (e.g., "5" = 5 minutes, "60" = 60 minutes)
            return int(s) * 60
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
        if SEED_KLINES_LIMIT < 26:
            logger.warning("SEED_KLINES_LIMIT is very low (%d); MACD requires >=26 for stability", SEED_KLINES_LIMIT)
        
        tfs = list(set(ROOT_TFS + MTF_TFS + MTF_ALIGN_TFS))
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
                
                logger.warning("[SEED_COMPLETE] %s %s: seeded with %d candles", symbol, tf, len(klines_sorted))
                
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
        """
        REST poller as fallback when WS is unavailable.
        Polls ROOT_TFS and short MTF TFs (5, 15) to ensure short-term data always available.
        """
        logger.info("REST poller started (interval=%s seconds) - PRIMARY FALLBACK FOR SHORT MTF DATA", REST_POLL_INTERVAL)
        poll_count = 0
        try:
            while not self._stop and (not USE_WS or not self.client.is_ws_connected()):
                poll_count += 1
                if poll_count % 5 == 0:
                    logger.info("[REST_POLLER] Active poll #%d, symbols=%d, USE_WS=%s, WS_CONNECTED=%s", 
                               poll_count, len(self.symbols), USE_WS, 
                               self.client.is_ws_connected() if hasattr(self.client, 'is_ws_connected') else "N/A")
                
                start = time.time()
                if not self.symbols:
                    await asyncio.sleep(REST_POLL_INTERVAL)
                    continue

                async def poll_symbol(sym: str):
                    async with self.request_sem:
                        # FIX #4: Poll correct TF format (numeric, not "5m" format)
                        tfs_to_poll = list(set(ROOT_TFS + ["5", "15"]))  # CORRECT: "5" not "5m"
                        
                        for tf in tfs_to_poll:
                            try:
                                # FIX #2: Use _call_get_klines for consistency
                                data = await self._call_get_klines(sym, tf, limit=3)
                                normalized = self._normalize_klines(data, tf) if data else []
                                
                                if normalized:
                                    lst = self.kline_store.get(sym, {}).get(tf, [])
                                    last_new = None
                                    
                                    for c in reversed(normalized):
                                        if c.get("close") is not None:
                                            last_new = {
                                                "start_at": c.get("start_at"), 
                                                "close": float(c.get("close")), 
                                                "volume": c.get("volume")
                                            }
                                            break
                                    
                                    if last_new:
                                        if lst:
                                            try:
                                                if lst[-1].get("start_at") == last_new.get("start_at"):
                                                    lst[-1] = last_new
                                                else:
                                                    lst.append(last_new)
                                            except Exception:
                                                self.kline_store.setdefault(sym, {})[tf] = [last_new]
                                        else:
                                            self.kline_store.setdefault(sym, {})[tf] = [last_new]
                                        
                                        if tf in ["5", "15"]:
                                            logger.debug("[REST_POLLER_UPDATE] %s %s: updated kline (close=%.8f)", sym, tf, last_new["close"])
                            except Exception:
                                logger.debug("REST poll kline failed for %s %s", sym, tf, exc_info=True)

                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(poll_symbol(s)) for s in batch]
                    
                    # FIX #3: Proper timeout handling instead of asyncio.wait
                    try:
                        await asyncio.wait_for(asyncio.gather(*tasks), timeout=REST_POLL_INTERVAL)
                    except asyncio.TimeoutError:
                        logger.debug("REST poller batch timeout after %s seconds", REST_POLL_INTERVAL)
                    except Exception:
                        pass

                elapsed = time.time() - start
                to_sleep = max(0, REST_POLL_INTERVAL - elapsed)
                
                if USE_WS and self.client.is_ws_connected():
                    logger.info("WS reconnected; stopping REST poller (WS is primary)")
                    break
                
                await asyncio.sleep(to_sleep)
        except asyncio.CancelledError:
            logger.info("REST poller cancelled")
        except Exception:
            logger.exception("REST poller encountered an exception")
        logger.info("REST poller stopped")

    async def _ensure_rest_poller(self):
        """
        Ensure REST poller is running as fallback when WS is unavailable.
        """
        if USE_WS and self.client.is_ws_connected():
            if self._rest_poller_task and not self._rest_poller_task.done():
                try:
                    self._rest_poller_task.cancel()
                    logger.info("[REST_POLLER_STOP] WS connected, stopping REST poller (WS is primary)")
                except Exception:
                    pass
                self._rest_poller_task = None
            return
        
        if self._rest_poller_task and not self._rest_poller_task.done():
            return
        
        logger.info("[REST_POLLER_START] WS unavailable, starting REST poller as fallback")
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
            result = zero_cross
            
            if DEBUG_SURGICAL_LOGS:
                logger.info("[FLIP_DEBUG] %s %s: prev=%.8f, cur=%.8f, change=%.8f, zero_cross=%s, FLIP=%s", 
                           symbol, tf, prev, cur, hist_change, zero_cross, result)
            
            if DEBUG_SURGICAL_LOGS and (symbol or tf):
                logger.info("[SURGICAL_LOG_4] FLIP_CHECK %s %s: prev=%.6f, cur=%.6f, flip=%s", 
                           symbol, tf, prev, cur, result)
            
            if result and DEBUG_SURGICAL_LOGS:
                logger.warning("[FLIP_DETECTED_INTERNAL] %s %s: STRONG FLIP! prev=%.8f -> cur=%.8f (change=%.8f)", 
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

                root_signals: List[Dict[str, Any]] = []
                logger.info("[DIAGNOSTIC] root_scan_loop: Starting symbol checks (total=%d)", len(self.symbols))

                async def check_symbol(sym: str):
                    try:
                        async with self.request_sem:
                            price = await self.client.get_latest_price(sym)
                        
                        if price is None:
                            try:
                                # FIX #5: Check ROOT_TFS is not empty before accessing [0]
                                if ROOT_TFS and USE_WS and self.client.is_ws_connected():
                                    ws_last = self.client.get_ws_latest_kline(sym, ROOT_TFS[0]) if hasattr(self.client, "get_ws_latest_kline") else None
                                    if ws_last and ws_last.get("close") is not None:
                                        price = float(ws_last.get("close"))
                            except Exception:
                                price = None
                        
                        if price is None:
                            return
                        
                        self._last_price_cache[sym] = price
                        
                        # FIX #5b: Optimize volume caching - only update every 5 symbols to save API calls
                        self._symbol_check_count = (self._symbol_check_count + 1) % 999
                        if self._symbol_check_count % 5 == 0:
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
                            
                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change
                                })
                                logger.info("SIGNAL DETECTED: %s %s @ %s", sym, root, price)
                                if DEBUG_SURGICAL_LOGS:
                                    logger.warning("[SIGNAL_DETECTED_CONFIRMED] %s %s price=%s flip=TRUE", sym, root, price)
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

                await self._check_monitored_symbols()

                if root_signals:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))
                    evaluated = await self.handle_root_signals(root_signals)
                else:
                    evaluated = []
                    logger.info("No root signals this interval.")
                
                await self.send_summary(root_signals, evaluated)
                
                try:
                    candidates_count = len(root_signals) if root_signals else 0
                    logger.info("ROOT_SCAN_COMPLETE: checked=%d, signals=%d, candidates=%d", 
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
                now = time.time()
                next_5m = math.ceil(now / 300.0) * 300.0
                to_sleep = max(0, next_5m - now)
                logger.debug("ROOT_SCAN_INTERVAL not set; sleeping until next 5m open in %.1fs", to_sleep)
                await asyncio.sleep(to_sleep)

    # ------------------------------------------------------------------
    # MTF Alignment helpers
    # ------------------------------------------------------------------

    def _compute_mtf_alignment(self, symbol: str, price: float) -> Dict[str, Any]:
        """
        FIX #6: Evaluate MTF alignment across MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
        using NUMERIC format matching Bybit API.

        Returns dict with status, per-TF states, negative_tfs list, and 1d slope.

        Scenarios:
          A — all TFs positive → "aligned"
          C — only D (daily) negative but histogram rising (upward slope) → "daily_rising"
          B — 1+ TFs negative (not meeting C) → "monitoring"
        """
        tf_states: Dict[str, Dict[str, Any]] = {}
        negative_tfs: List[str] = []
        one_d_hist: List[float] = []

        logger.warning("[MTF_ALIGN_START] Computing MTF alignment for %s @ price=%s", symbol, price)

        for tf in MTF_ALIGN_TFS:
            _, _, hist = self.compute_macd_for(symbol, tf, include_price=price, use_ws_current=True)
            hist = hist or []
            cur  = hist[-1] if hist else None
            prev = hist[-2] if len(hist) >= 2 else None
            is_positive = cur is not None and cur > 0
            is_flip     = (prev is not None and prev < 0 and cur is not None and cur > 0)
            
            numeric_count = sum(1 for v in hist if v is not None)
            logger.warning(
                "[MTF_ALIGN_TRACE] TF=%s | hist_len=%d | numeric_count=%d | prev=%s | cur=%s | is_positive=%s | is_flip=%s",
                tf,
                len(hist),
                numeric_count,
                f"{prev:.8f}" if prev is not None else "None",
                f"{cur:.8f}" if cur is not None else "None",
                is_positive,
                is_flip
            )
            
            tf_states[tf] = {
                "cur": cur, "prev": prev,
                "is_positive": is_positive, "is_flip": is_flip, "slope": None,
            }
            if tf == "D":
                one_d_hist = hist
            if not is_positive:
                negative_tfs.append(tf)
                logger.warning("[MTF_ALIGN_TRACE] → TF %s appended to negative_tfs (is_positive=%s)", tf, is_positive)

        logger.warning("[MTF_ALIGN_DECISION] negative_tfs=%s (len=%d)", negative_tfs, len(negative_tfs))

        # Scenario A: all TFs positive
        if not negative_tfs:
            logger.warning("[MTF_ALIGN_RESULT] %s → ALIGNED (Scenario A: all TFs positive)", symbol)
            return {"status": "aligned", "tfs": tf_states, "negative_tfs": [], "one_d_slope": None}

        # Scenario C: only D is negative but rising
        if negative_tfs == ["D"]:
            one_d_slope = slope(one_d_hist, lookback=MTF_SLOPE_LOOKBACK) if one_d_hist else None
            logger.warning("[MTF_ALIGN_TRACE] Scenario C check: one_d_slope=%s", one_d_slope)
            if one_d_slope is not None and one_d_slope > 0:
                tf_states["D"]["slope"] = one_d_slope
                logger.warning("[MTF_ALIGN_RESULT] %s → DAILY_RISING (Scenario C: only D negative but slope=%.8f > 0)", symbol, one_d_slope)
                return {
                    "status": "daily_rising",
                    "tfs": tf_states,
                    "negative_tfs": ["D"],
                    "one_d_slope": one_d_slope,
                }
            else:
                logger.warning("[MTF_ALIGN_TRACE] Scenario C failed: one_d_slope not positive (slope=%s)", one_d_slope)

        # Scenario B: 1+ TFs negative and Scenario C not met
        logger.warning("[MTF_ALIGN_RESULT] %s → MONITORING (Scenario B: negative_tfs=%s)", symbol, negative_tfs)
        return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}

    def _build_mtf_state_str(self, tf_states: Dict[str, Any]) -> str:
        """
        Build compact MTF state string for Telegram.
        Example: '5✅ 15🔄 60✅ 240❌ D📈'

        Legend:
          ✅  — positive histogram
          🔄  — just flipped positive (prev<0 → cur>0)
          📈  — negative but rising slope (D Scenario C)
          ❌  — negative
        """
        parts = []
        for tf in MTF_ALIGN_TFS:
            d = tf_states.get(tf, {})
            if d.get("is_flip"):
                parts.append(f"{tf}🔄")
            elif d.get("is_positive"):
                parts.append(f"{tf}✅")
            elif tf == "D" and d.get("slope") is not None and d.get("slope", 0) > 0:
                parts.append(f"{tf}📈")
            else:
                parts.append(f"{tf}❌")
        return " ".join(parts)

    async def _check_monitored_symbols(self):
        """
        Scenario B monitor: re-evaluate symbols waiting for their last negative TF to flip.
        When resolved, queue for trade opening via handle_root_signals.
        """
        if not self._mtf_monitoring:
            return

        MONITORING_MAX_AGE = 86400
        now = time.time()
        to_remove: List[str] = []
        newly_aligned: List[Dict[str, Any]] = []

        for sym, info in list(self._mtf_monitoring.items()):
            try:
                if now - info.get("started_at", now) > MONITORING_MAX_AGE:
                    logger.info("MONITORING EXPIRED (24h): %s — removing", sym)
                    to_remove.append(sym)
                    continue

                price = self._last_price_cache.get(sym)
                if price is None:
                    try:
                        async with self.request_sem:
                            price = await self.client.get_latest_price(sym)
                        if price:
                            self._last_price_cache[sym] = price
                    except Exception:
                        pass
                if price is None:
                    continue

                mtf_align = self._compute_mtf_alignment(sym, price)
                status = mtf_align["status"]

                if status in ("aligned", "daily_rising"):
                    logger.info("MONITORING RESOLVED: %s → %s — queuing trade open", sym, status)
                    to_remove.append(sym)
                    newly_aligned.append({
                        "symbol": sym,
                        "root": info["root"],
                        "price": price,
                        "hist": [],
                        "vol_change": self.compute_24h_volume_change(sym),
                        "from_monitoring": True,
                    })
                else:
                    prev_neg = set(info.get("negative_tfs", []))
                    curr_neg = set(mtf_align.get("negative_tfs", []))
                    if curr_neg != prev_neg:
                        self._mtf_monitoring[sym]["negative_tfs"] = list(curr_neg)
                        self._mtf_monitoring[sym]["last_alert"] = now
            except Exception:
                logger.exception("Error checking monitored symbol %s", sym)

        for sym in to_remove:
            self._mtf_monitoring.pop(sym, None)

        if newly_aligned:
            await self.handle_root_signals(newly_aligned)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate MTF alignment for each root signal and act on each scenario.

        Scenario A  (aligned)       → open trade if TRADE_ENABLED
        Scenario C  (daily_rising)  → open trade
        Scenario B  (monitoring)    → add to watch-list until last flip
        """
        evaluated: List[Dict[str, Any]] = []
        to_open:   List[Dict[str, Any]] = []

        for item in root_signals:
            sym        = item["symbol"]
            price      = item["price"]
            root       = item["root"]
            vol_change = item.get("vol_change")

            hist = item.get("hist", [])
            if not hist:
                _, _, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            # FIX #6: Compute MTF alignment with correct TF format
            mtf_align    = self._compute_mtf_alignment(sym, price)
            mtf_status   = mtf_align["status"]
            negative_tfs = mtf_align.get("negative_tfs", [])

            score  = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive"))
            score += sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))
            if vol_change is not None and vol_change > 0:
                score += min(vol_change, 1.0)

            entry: Dict[str, Any] = {
                "symbol":       sym,
                "root":         root,
                "price":        price,
                "hist":         hist,
                "macd_hist_val": macd_hist_val,
                "mtf":          mtf_align["tfs"],
                "mtf_status":   mtf_status,
                "negative_tfs": negative_tfs,
                "one_d_slope":  mtf_align.get("one_d_slope"),
                "vol_change":   vol_change,
                "score":        score,
                "accept":       False,
                "reason":       "pending",
            }

            if mtf_status in ("aligned", "daily_rising"):
                entry["accept"] = True
                entry["reason"] = mtf_status
                to_open.append(entry)
                logger.info("MTF %s → ACCEPT: %s %s score=%.2f", mtf_status, sym, root, score)

            elif mtf_status == "monitoring":
                entry["reason"] = "monitoring"
                if sym not in self._mtf_monitoring:
                    self._mtf_monitoring[sym] = {
                        "root":        root,
                        "price":       price,
                        "started_at":  time.time(),
                        "negative_tfs": list(negative_tfs),
                        "last_alert":  0.0,
                    }
                    logger.info("MTF MONITORING: %s added — waiting on: %s", sym, negative_tfs)

            evaluated.append(entry)

        await self._emit_event("candidates_evaluated", evaluated)

        # FIX #8: Log candidates summary
        logger.warning(
            "[CANDIDATES_SUMMARY] Total evaluated=%d, Accepted/To Open=%d, Monitoring now=%d",
            len(evaluated), len(to_open), len(self._mtf_monitoring)
        )

        candidates = to_open
        if ROOT_FILTER:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for c in candidates:
                grouped.setdefault(c["root"], []).append(c)
            selected: List[Dict[str, Any]] = []
            for rt in ROOT_TFS:
                lst = grouped.get(rt, [])
                if not lst:
                    continue
                top = sorted(lst, key=lambda r: r["score"], reverse=True)[:ROOT_TOP_N]
                selected.extend(top)
            candidates = sorted(selected, key=lambda r: r["score"], reverse=True)

        current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
        logger.info(
            "Candidates to open: %d (MAX_OPEN_TRADES=%d, currently_open=%d)",
            len(candidates), MAX_OPEN_TRADES, current_open,
        )

        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Max open trades reached — halting further opens.")
                break
            sym   = c["symbol"]
            price = c["price"]
            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None
            symbol_info = await self.client.get_symbol_info(sym)
            qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
            qty     = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
            if qty <= 0 or math.isclose(qty, 0.0):
                logger.warning("Zero qty for %s after quantize — skipping.", sym)
                continue
            if qty != qty_raw:
                logger.debug(
                    "Qty for %s adjusted %s → %s (step=%s min=%s)",
                    sym, qty_raw, qty, symbol_info.get("step"), symbol_info.get("min_qty"),
                )
            side       = "Buy"
            reason_tag = c.get("reason", "signal")
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    await send_message(
                        f"✅ Trade Opened — {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Score: {c['score']:.2f} | Reason: {reason_tag}"
                    )
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
            else:
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"]})
                logger.info("Simulated open %s qty=%s score=%.2f", sym, qty, c["score"])
                await send_message(
                    f"📊 Simulated Trade — {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Score: {c['score']:.2f} | Reason: {reason_tag}"
                )
            self._mtf_monitoring.pop(sym, None)

        return evaluated

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None):
        """
        Telegram summary layout with MTF alignment visualization.
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())

        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = sig.get("root", "?")
            tf_counts[rt] = tf_counts.get(rt, 0) + 1

        window_map = {"60": 30, "240": 12, "D": 5}
        header_lines = [f"📊 Bybit Perp Root Summary — {now_str}"]
        for rt in ROOT_TFS:
            cnt = tf_counts.get(rt, 0)
            win = window_map.get(rt)
            if cnt:
                if win:
                    header_lines.append(f"  {rt}: {cnt} (window: {win})")
                else:
                    header_lines.append(f"  {rt}: {cnt}")

        header_lines.append("")
        header_lines.append("Signals (all root TF signals):")
        if not root_signals:
            header_lines.append("  None")
        else:
            for sig in root_signals:
                sym = sig.get("symbol")
                rt = sig.get("root")
                price = sig.get("price")
                try:
                    if price >= 1000:
                        price_str = f"${price:,.2f}"
                    elif price >= 1:
                        price_str = f"${price:.4f}"
                    else:
                        price_str = f"${price:.8f}"
                except Exception:
                    price_str = str(price)
                header_lines.append(f"  - {sym} | {rt} | {price_str}")

        first_block = "\n".join(header_lines)
        try:
            await send_message(first_block)
        except Exception:
            logger.exception("Failed to send first summary block")

        signal_tfs = [rt for rt in ROOT_TFS if rt in tf_counts]
        for rt in signal_tfs:
            tf_sigs = [s for s in root_signals if s.get("root") == rt]
            tf_sigs_sorted = sorted(
                tf_sigs,
                key=lambda s: eval_map.get((s["symbol"], s["root"]), {}).get("score", 0.0),
                reverse=True,
            )
            for sig in tf_sigs_sorted:
                try:
                    sym        = sig["symbol"]
                    price      = sig["price"]
                    vol_change = sig.get("vol_change")
                    ev         = eval_map.get((sym, rt), {})

                    macd_hist_val = float(ev.get("macd_hist_val") or 0.0)
                    score         = float(ev.get("score")         or 0.0)
                    # FIX #7: Removed redundant compute, use evaluated value directly
                    mtf_status    = ev.get("mtf_status", "unknown")
                    mtf_tfs       = ev.get("mtf", {})

                    negative_tfs  = ev.get("negative_tfs", [])
                    one_d_slope   = ev.get("one_d_slope")

                    if price >= 1000:
                        price_str = f"${price:,.2f}"
                    elif price >= 1:
                        price_str = f"${price:.4f}"
                    else:
                        price_str = f"${price:.8f}"

                    vol_str = "N/A"
                    if vol_change is not None:
                        try:
                            vol_str = f"{vol_change * 100:+.1f}%"
                        except Exception:
                            vol_str = str(vol_change)

                    mtf_state_str = self._build_mtf_state_str(mtf_tfs) if mtf_tfs else "N/A"

                    if mtf_status == "aligned":
                        state_str = "✅ Aligned (Accept)"
                    elif mtf_status == "daily_rising":
                        slope_note = f" (D slope: {one_d_slope:+.4f})" if one_d_slope is not None else ""
                        state_str = f"📈 Daily Rising (Accept){slope_note}"
                    elif mtf_status == "monitoring":
                        state_str = f"⏳ Monitoring (Not accepted yet)"
                    else:
                        state_str = "❓ Unknown"

                    block = "\n".join([
                        f"📌 Bybit Perp | {rt} Signal",
                        f"Symbol: {sym}",
                        f"Price: {price_str}",
                        f"MACD H: {macd_hist_val:+.6f}",
                        f"24h Vol Δ: {vol_str}",
                        f"MTF State: {mtf_state_str}",
                        f"Status: {state_str}",
                    ])
                    await send_message(block)
                except Exception:
                    logger.exception("Failed to send signal block for %s %s", sig.get("symbol"), rt)

        try:
            current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
            remaining = max(0, MAX_OPEN_TRADES - current_open)
            accepted = []
            if evaluated:
                for e in evaluated:
                    if e.get("accept"):
                        accepted.append(e)
            accepted_sorted = sorted(accepted, key=lambda r: r.get("score", 0.0), reverse=True)
            recommended = accepted_sorted[:remaining] if remaining > 0 else []

            rec_lines = [f"📣 Recommended Signals for Trading — {now_str}"]
            rec_lines.append(f"Open trades: {current_open} / {MAX_OPEN_TRADES}")
            rec_lines.append(f"Slots available: {remaining}")
            if not recommended:
                rec_lines.append("No recommended signals to open at this time.")
            else:
                for r in recommended:
                    sym = r["symbol"]
                    rt = r["root"]
                    price = r["price"]
                    score = r.get("score", 0.0)
                    if price >= 1000:
                        price_str = f"${price:,.2f}"
                    elif price >= 1:
                        price_str = f"${price:.4f}"
                    else:
                        price_str = f"${price:.8f}"
                    rec_lines.append(f"  - {sym} | {rt} | {price_str} | score={score:.2f}")

            await send_message("\n".join(rec_lines))
        except Exception:
            logger.exception("Failed to send recommended signals block")

    async def run(self):
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
