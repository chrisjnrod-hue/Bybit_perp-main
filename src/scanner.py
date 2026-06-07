# Diagnostic scanner - replace existing scanner.py with this complete file
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

# ============ NEW: Diagnostic flags ============
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# TFs to scan for acceptance/alerts (per your request)
SCAN_TFS = ["5m", "15m", "1h", "4h", "1d"]
# Detailed alert ordering: 1h, then 4h, then 1d
ALERT_ORDER = ["1h", "4h", "1d"]

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

                # IMPORTANT:
                # Keep current candle for live MACD flip detection

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
            # Accept flip when prev <= 0 < cur
            zero_cross = prev <= 0 and cur > 0
            result = zero_cross
            if DEBUG_SURGICAL_LOGS:
                logger.info("[FLIP_DEBUG] %s %s: prev=%.8f, cur=%.8f, zero_cross=%s, flip=%s", 
                           symbol, tf, prev, cur, zero_cross, result)
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
                                if USE_WS and self.client.is_ws_connected():
                                    ws_last = self.client.get_ws_latest_kline(sym, SCAN_TFS[0]) if hasattr(self.client, "get_ws_latest_kline") else None
                                    if ws_last and ws_last.get("close") is not None:
                                        price = float(ws_last.get("close"))
                            except Exception:
                                price = None
                        
                        if price is None:
                            return
                        
                        self._last_price_cache[sym] = price
                        await self._update_24h_volume(sym)

                        # Evaluate multi-timeframe MACD state
                        mtf_state = {}
                        positive_count = 0
                        flips = []
                        negative_tfs = []

                        last_hist_vals = {}

                        for tf in SCAN_TFS:
                            try:
                                _, _, hist = self.compute_macd_for(sym, tf, include_price=price, use_ws_current=True)
                                cur = hist[-1] if hist and len(hist) >= 1 else None
                                prev = hist[-2] if hist and len(hist) >= 2 else None
                                mtf_state[tf] = {"prev": prev, "cur": cur}
                                last_hist_vals[tf] = cur

                                if cur is not None and cur > 0:
                                    positive_count += 1
                                else:
                                    negative_tfs.append(tf)

                                if prev is not None and cur is not None and prev <= 0 and cur > 0:
                                    flips.append(tf)
                            except Exception:
                                logger.exception("MACD compute failed for %s %s", sym, tf)

                        # Decision logic as requested:
                        accept = False
                        reason = "none"
                        # Accept if all TFs positive
                        if positive_count == len(SCAN_TFS):
                            accept = True
                            reason = "all_positive"
                        # Or accept if any TF has a current flip
                        elif flips:
                            accept = True
                            reason = f"flip_in_{','.join(flips)}"
                        # Or if only 1d is negative but 1d histogram slope is positive (rising)
                        elif negative_tfs == ["1d"]:
                            try:
                                _, _, h1d = self.compute_macd_for(sym, "1d", include_price=price, use_ws_current=True)
                                one_d_slope = slope(h1d or [], lookback=MTF_SLOPE_LOOKBACK) if h1d else None
                                if one_d_slope and one_d_slope > 0:
                                    accept = True
                                    reason = "1d_negative_but_rising"
                            except Exception:
                                logger.exception("1d slope check failed for %s", sym)

                        vol_change = self.compute_24h_volume_change(sym)
                        # Build signal record only if accept True OR if a flip exists (we want to alert on flips too)
                        if accept or flips:
                            root_signals.append({
                                "symbol": sym,
                                "price": price,
                                "mtf_state": mtf_state,
                                "positive_count": positive_count,
                                "flips": flips,
                                "negative_tfs": negative_tfs,
                                "accept": accept,
                                "reason": reason,
                                "vol_change": vol_change,
                                "last_hist_vals": last_hist_vals
                            })
                            logger.info("SIGNAL DETECTED: %s reason=%s price=%s flips=%s positives=%d", sym, reason, price, flips, positive_count)

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
                    "[SCAN_RESULTS] Checked=%d symbols, Signals=%d, SCAN_TFS=%s",
                    checked_count,
                    len(root_signals),
                    SCAN_TFS
                )

                logger.info("[DIAGNOSTIC] root_scan_loop: Checked %d symbols, found %d signals", checked_count, len(root_signals))
                await self._emit_event("root_signals", root_signals)

                if root_signals:
                    # We no longer trigger any MTF subscribe-filter; MTF filter removed as requested.
                    await self.handle_root_signals(root_signals)
                else:
                    logger.info("No root signals this interval.")
                await self.send_summary(root_signals)
                
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

    
    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]):
        # No MTF filtering: accept candidates are already marked in signal["accept"]
        for sig in root_signals:
            try:
                sym = sig["symbol"]
                price = sig["price"]
                accept = sig.get("accept", False)
                reason = sig.get("reason", "")
                if accept:
                    if not self.trade_manager.can_open():
                        logger.info("Cannot open more trades; skipping %s", sym)
                        continue
                    if not TRADE_ENABLED:
                        logger.info("TRADE_ENABLED is False; not opening trade for %s", sym)
                        continue
                    # Attempt to open a trade using trade_manager if method exists
                    try:
                        open_fn = getattr(self.trade_manager, "open_trade", None) or getattr(self.trade_manager, "open", None)
                        if open_fn:
                            res = open_fn(sym, price)
                            if inspect.isawaitable(res):
                                await res
                            logger.info("Trade open requested for %s at %s (reason=%s)", sym, price, reason)
                        else:
                            logger.warning("Trade manager doesn't expose open_trade/open - cannot open automatically for %s", sym)
                    except Exception:
                        logger.exception("Failed to request trade open for %s", sym)
                else:
                    # Not accepted yet - monitoring state; do nothing here
                    logger.info("Signal for %s not accepted yet (reason=%s); monitoring", sig.get("symbol"), reason)
            except Exception:
                logger.exception("Error handling root signal %s", sig)

    async def send_summary(self, root_signals: List[Dict[str, Any]]):
        # Build telegram message per your requested structure:
        # 1) First block: root tfs signal summary in one block
        # 2) Then blocks per 1h signals (BYBIT PERP block format), then 4h, then 1d
        try:
            if not root_signals:
                await send_message("SCAN SUMMARY\nNo signals this interval.")
                return

            # First block: summary header + one-line per signal describing primary info
            summary_lines = ["ROOT TF SIGNAL SUMMARY"]
            for s in root_signals:
                # We choose representative root as the highest TF where flip/positive detected:
                positives = [tf for tf,st in (s.get("mtf_state") or {}).items() if st.get("cur") is not None and st.get("cur") > 0]
                flips = s.get("flips") or []
                summary_lines.append(f"{s['symbol']} | positives={len(positives)} | flips={','.join(flips) if flips else '-'} | reason={s.get('reason')}")
            blocks = ["\n".join(summary_lines)]

            # Helper to format detailed block for a single signal
            def format_signal_block(sig: Dict[str, Any]) -> str:
                symbol = sig["symbol"]
                price = sig["price"]
                vol_change = sig.get("vol_change") or 0.0
                # Signal strength: combine last 1h hist magnitude (if present) + 24h vol change as a simple metric
                last_hist_vals = sig.get("last_hist_vals") or {}
                # Prefer 1h hist as representative strength, fallback to largest abs hist
                representative_hist = None
                if "1h" in last_hist_vals and last_hist_vals["1h"] is not None:
                    representative_hist = last_hist_vals["1h"]
                else:
                    # pick any non-None hist with largest abs value
                    hist_candidates = {k: v for k, v in last_hist_vals.items() if v is not None}
                    if hist_candidates:
                        representative_hist = max(hist_candidates.values(), key=lambda x: abs(x))
                hist_strength_pct = (abs(representative_hist) * 100) if representative_hist is not None else 0.0
                vol_strength_pct = (vol_change * 100) if vol_change is not None else 0.0
                overall_strength = hist_strength_pct + vol_strength_pct

                # MTF alignment: simple textual representation
                mtf_state = sig.get("mtf_state") or {}
                alignment_parts = []
                for tf in ["5m","15m","1h","4h","1d"]:
                    st = mtf_state.get(tf)
                    if not st:
                        alignment_parts.append(f"{tf}:?")
                    else:
                        cur = st.get("cur")
                        if cur is None:
                            alignment_parts.append(f"{tf}:?")
                        elif cur > 0:
                            alignment_parts.append(f"{tf}:+")
                        else:
                            alignment_parts.append(f"{tf}:-")
                alignment = " ".join(alignment_parts)

                block = (
f"BYBIT PERP\n"
f"Symbol: {symbol}\n"
f"Price: {price}\n"
f"Signal Strength: {overall_strength:.1f}\n"
f"MTF Alignment: {alignment}\n"
f"Reason: {sig.get('reason')}\n"
                )
                return block

            # Add details in order: 1h first (one block per 1h signal), then 4h, then 1d
            for tf_group in ALERT_ORDER:
                tf_items = [s for s in root_signals if (s.get("mtf_state") or {}).get(tf_group) is not None]
                # Prefer signals where the tf_group shows positive or flip
                ordered_items = sorted(tf_items, key=lambda s: (0 if (s.get("mtf_state") or {}).get(tf_group, {}).get("cur") and (s.get("mtf_state") or {}).get(tf_group, {}).get("cur") > 0 else 1, -(s.get("vol_change") or 0)))
                for sig in ordered_items:
                    blocks.append(format_signal_block(sig))

            message = "\n\n".join(blocks)
            await send_message(message)
        except Exception:
            logger.exception("Failed to build/send summary")

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
