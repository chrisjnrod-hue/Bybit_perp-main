# Add this COMPLETE diagnostic version to replace your current scanner.py
# This will help us identify exactly where the process breaks down

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
    MACD_HIST_THRESHOLD, VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT
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

# ============ MTF Alignment TFs — explicit 5-TF alignment check ============
# These are checked in order for Scenarios A / B / C regardless of MTF_TFS config.
MTF_ALIGN_TFS = ["5m", "15m", "1h", "4h", "1d"]


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
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}  # Scenario B watch-list
        # ---- Signal dedup & repeat tracking ----
        # Keyed by (symbol, root_tf) → last MTF alignment snapshot sent
        # A signal is repeated only when the MTF alignment snapshot changes
        self._sent_signal_mtf: Dict[tuple, str] = {}
        logger.info("scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d MAX_CONCURRENT=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s VOLUME_FILTER=%s)", 
                   bool(USE_WS), SEED_KLINES_LIMIT, MAX_CONCURRENT_REQUESTS, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE, VOLUME_FILTER_ENABLED)

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
            # ============ IMPROVED FLIP DETECTION WITH NOISE FILTER ============
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
        """
        Fetch 24h ticker for symbol and cache both the current and previous 24h volume.
        Returns the 24h volume-change percentage (decimal) or None on failure.

        Priority:
          1. Use prevVolume24h from Bybit API when present  → exact window comparison
          2. Fall back to comparing successive scanner-poll snapshots (same as before)
        """
        try:
            now = time.time()
            # Rate-limit cache refresh to once per 60 s per symbol
            if symbol in self._last_price_time and (now - self._last_price_time[symbol]) < 60:
                return self.compute_24h_volume_change(symbol)

            ticker = None
            if hasattr(self.client, "get_24h_ticker"):
                async with self.request_sem:
                    ticker = await self.client.get_24h_ticker(symbol)

            if ticker and isinstance(ticker, dict):
                vol24h   = ticker.get("volume24h")
                prev_vol = ticker.get("prevVolume24h")
                vol_pct  = ticker.get("volume24hPcnt")   # pre-computed when prev_vol present

                if vol24h is not None:
                    if symbol not in self._24h_volumes:
                        self._24h_volumes[symbol] = {"current": vol24h, "previous": vol24h}
                    else:
                        self._24h_volumes[symbol]["previous"] = (
                            prev_vol if prev_vol is not None
                            else self._24h_volumes[symbol]["current"]
                        )
                        self._24h_volumes[symbol]["current"] = vol24h

                    # Store pre-computed pct when API gives it directly
                    if vol_pct is not None:
                        self._24h_volumes[symbol]["api_pct"] = vol_pct

                    self._last_price_time[symbol] = now
                    return self.compute_24h_volume_change(symbol)

            # Fallback: try get_24h_ticker's generic volume field
            if hasattr(self.client, "get_24h_ticker"):
                pass  # already tried above
            elif hasattr(self.client, "get_24h_ticker"):
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
                            return self.compute_24h_volume_change(symbol)
                        except Exception:
                            pass
        except Exception:
            logger.debug("Could not update 24h volume for %s", symbol, exc_info=True)
        return None

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        """
        Return 24h volume change as a decimal percentage (e.g. 0.05 = +5 %).
        Prefers the API-provided value (prevVolume24h comparison) over
        the fallback successive-poll comparison.
        Returns None when data is unavailable.
        """
        try:
            if symbol not in self._24h_volumes:
                return None
            vol_data = self._24h_volumes[symbol]

            # API pre-computed pct (most accurate)
            if "api_pct" in vol_data and vol_data["api_pct"] is not None:
                return float(vol_data["api_pct"])

            prev_vol = vol_data.get("previous", 0)
            curr_vol = vol_data.get("current", 0)
            if prev_vol is None or prev_vol <= 0:
                return None
            change = (curr_vol - prev_vol) / prev_vol
            return change
        except Exception:
            logger.debug("Could not compute 24h volume change for %s", symbol)
            return None

    # ------------------------------------------------------------------
    # Signal strength & volume gate helpers
    # ------------------------------------------------------------------

    def _signal_strength_rating(self, macd_hist_val: float, vol_change: Optional[float]) -> Dict[str, Any]:
        """
        Combine MACD histogram momentum and 24h volume change % into a single
        signal strength score and label.

        Score components (both normalised to 0-1 range then combined):
          • MACD component : clamped by MACD_HIST_THRESHOLD baseline
          • Volume component: positive vol_change contribution (max 1.0)

        Returns dict with:
          score        : float 0-2 (higher = stronger)
          label        : str  "🔥 Strong" | "🟡 Moderate" | "⭕ Weak"
          macd_rating  : str  descriptive MACD momentum
          vol_rating   : str  descriptive volume momentum
        """
        # MACD component — relative to threshold baseline
        threshold = max(float(MACD_HIST_THRESHOLD), 0.0)
        macd_excess = max(0.0, float(macd_hist_val) - threshold)
        # Normalise: treat 5× threshold (or 0.001 if threshold=0) as "full" MACD score
        ref = threshold * 5 if threshold > 0 else 0.001
        macd_score = min(macd_excess / ref, 1.0) if ref > 0 else 0.0

        # Volume component
        vol_score = 0.0
        if vol_change is not None and vol_change > 0:
            vol_score = min(float(vol_change), 1.0)  # cap at 100 %

        combined = macd_score + vol_score   # range 0 – 2

        if combined >= 1.4:
            label = "🔥 Strong"
        elif combined >= 0.6:
            label = "🟡 Moderate"
        else:
            label = "⭕ Weak"

        # MACD descriptive
        if macd_hist_val >= threshold * 3 if threshold > 0 else macd_hist_val >= 0.005:
            macd_rating = "Surging"
        elif macd_hist_val > threshold:
            macd_rating = "Rising"
        else:
            macd_rating = "Crossing"

        # Volume descriptive
        if vol_change is None:
            vol_rating = "Unknown"
        elif vol_change >= 0.30:
            vol_rating = f"+{vol_change * 100:.1f}% 🔥"
        elif vol_change >= 0.05:
            vol_rating = f"+{vol_change * 100:.1f}% 📈"
        elif vol_change > 0:
            vol_rating = f"+{vol_change * 100:.1f}%"
        else:
            vol_rating = f"{vol_change * 100:.1f}% ⚠️"

        return {
            "score":       combined,
            "label":       label,
            "macd_rating": macd_rating,
            "vol_rating":  vol_rating,
        }

    def _passes_volume_gate(self, vol_change: Optional[float]) -> bool:
        """
        Trade-open volume gate (never used to reject signals from Telegram).
        Returns True when vol_change satisfies the filter criteria, False blocks the open.

        Controlled by env vars:
          VOLUME_FILTER_ENABLED  (default true)
          VOLUME_MIN_CHANGE_PCT  (default 0.0 → any positive change passes)
        """
        if not VOLUME_FILTER_ENABLED:
            return True
        if vol_change is None:
            # Cannot confirm volume is positive — default allow (conservative)
            return True
        return vol_change > VOLUME_MIN_CHANGE_PCT

    def _mtf_snapshot_key(self, mtf_align: Dict[str, Any]) -> str:
        """
        Produce a compact string fingerprint of the current MTF alignment state.
        A signal is re-sent only when this key changes vs the last sent snapshot.
        """
        tfs = mtf_align.get("tfs", {})
        parts = []
        for tf in MTF_ALIGN_TFS:
            d = tfs.get(tf, {})
            if d.get("is_flip"):
                parts.append(f"{tf}:flip")
            elif d.get("is_positive"):
                parts.append(f"{tf}:pos")
            elif tf == "1d" and d.get("slope", 0) and d["slope"] > 0:
                parts.append(f"{tf}:rising")
            else:
                parts.append(f"{tf}:neg")
        return "|".join(parts)

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
                            
                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change
                                })
                                logger.info("Ã¢Å“â€œ SIGNAL DETECTED: %s %s @ %s", sym, root, price)
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

                # Re-evaluate symbols queued from a prior cycle's Scenario B (monitoring)
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
        Evaluate MTF alignment across MTF_ALIGN_TFS = [5m, 15m, 1h, 4h, 1d].

        Returns dict:
          status       : "aligned" | "daily_rising" | "monitoring"
          tfs          : per-TF state dicts (cur, prev, is_positive, is_flip, slope)
          negative_tfs : list of TF names with non-positive histogram
          one_d_slope  : 1d slope value (Scenario C only, else None)

        Scenarios:
          A — all TFs positive (a flip, prev<0→cur>0, counts as positive) → "aligned"
          C — only 1d negative but histogram rising (upward slope)        → "daily_rising"
          B — 1+ TFs negative (not meeting C)                             → "monitoring"
        """
        tf_states: Dict[str, Dict[str, Any]] = {}
        negative_tfs: List[str] = []
        one_d_hist: List[float] = []

        for tf in MTF_ALIGN_TFS:
            _, _, hist = self.compute_macd_for(symbol, tf, include_price=price, use_ws_current=True)
            hist = hist or []
            cur  = hist[-1] if hist else None
            prev = hist[-2] if len(hist) >= 2 else None
            is_positive = cur is not None and cur > 0
            is_flip     = (prev is not None and prev < 0 and cur is not None and cur > 0)
            tf_states[tf] = {
                "cur": cur, "prev": prev,
                "is_positive": is_positive, "is_flip": is_flip, "slope": None,
            }
            if tf == "1d":
                one_d_hist = hist
            if not is_positive:
                negative_tfs.append(tf)

        # Scenario A: all TFs positive
        if not negative_tfs:
            return {"status": "aligned", "tfs": tf_states, "negative_tfs": [], "one_d_slope": None}

        # Scenario C: only 1d is negative but rising
        if negative_tfs == ["1d"]:
            one_d_slope = slope(one_d_hist, lookback=MTF_SLOPE_LOOKBACK) if one_d_hist else None
            if one_d_slope is not None and one_d_slope > 0:
                tf_states["1d"]["slope"] = one_d_slope
                return {
                    "status": "daily_rising",
                    "tfs": tf_states,
                    "negative_tfs": ["1d"],
                    "one_d_slope": one_d_slope,
                }

        # Scenario B: 1+ TFs negative and Scenario C not met
        return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}

    def _build_mtf_state_str(self, tf_states: Dict[str, Any], scenario: str = "") -> str:
        """
        Build compact MTF state string for Telegram.
        Example: '5m✅ 15m🔄 1h✅ 4h❌ 1d📈'

        Icons are strictly derived from the active MTF alignment scenario:

          Scenario A ("aligned")      — all TFs positive:
            ✅  positive histogram  |  🔄  flipped positive this candle

          Scenario C ("daily_rising") — only 1d negative but rising slope:
            ✅ / 🔄 for positive TFs  |  📈  for 1d (negative but slope > 0)

          Scenario B ("monitoring")   — 1+ TFs negative, Scenario C not met:
            ✅ / 🔄 for positive TFs  |  ❌  for any negative TF

        The `scenario` param (A/B/C status string) gates which icons are allowed
        so the displayed row can never contradict the evaluated outcome.
        """
        parts = []
        for tf in MTF_ALIGN_TFS:
            d = tf_states.get(tf, {})
            is_positive = d.get("is_positive", False)
            is_flip     = d.get("is_flip", False)

            if is_flip:
                # Flip (prev<0 → cur>0) is a positive state in all scenarios
                parts.append(f"{tf}🔄")
            elif is_positive:
                # Straightforward positive histogram — valid in all scenarios
                parts.append(f"{tf}✅")
            elif tf == "1d" and scenario == "daily_rising":
                # Scenario C only: 1d is negative but rising — show 📈
                # (slope > 0 is already guaranteed by _compute_mtf_alignment before
                #  returning status="daily_rising", so we don't re-check it here)
                parts.append(f"{tf}📈")
            else:
                # Negative in Scenario B (or 1d negative in Scenario C for any
                # tf other than 1d, which can't happen but is safe to handle)
                parts.append(f"{tf}❌")
        return " ".join(parts)

    async def _check_monitored_symbols(self):
        """
        Scenario B monitor: re-evaluate symbols waiting for their last negative TF
        to flip positive on the current candle open.

        - When all TFs align (A or C)  → opens trade via handle_root_signals
        - When a partial flip occurs   → sends Telegram update (rate-limited, 5 min)
        - Entries older than 24 h      → expired and removed
        """
        if not self._mtf_monitoring:
            return

        MONITORING_MAX_AGE    = 86400  # 24 hours
        PARTIAL_ALERT_COOLDOWN = 300   # 5 minutes between partial alerts
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
                    tf_str = self._build_mtf_state_str(mtf_align["tfs"], scenario=status)
                    await send_message(
                        f"✅ MTF Aligned — {sym}\n"
                        f"Root: {info['root']} | Price: {price}\n"
                        f"Status: {status.replace('_', ' ').title()}\n"
                        f"MTF: {tf_str}\n"
                        f"Opening trade..."
                    )
                    newly_aligned.append({
                        "symbol": sym,
                        "root": info["root"],
                        "price": price,
                        "hist": [],
                        "vol_change": self.compute_24h_volume_change(sym),
                        "from_monitoring": True,
                    })
                else:
                    # Check for partial progress
                    prev_neg = set(info.get("negative_tfs", []))
                    curr_neg = set(mtf_align.get("negative_tfs", []))
                    if curr_neg != prev_neg:
                        self._mtf_monitoring[sym]["negative_tfs"] = list(curr_neg)
                        newly_flipped = prev_neg - curr_neg
                        if newly_flipped and (now - info.get("last_alert", 0) > PARTIAL_ALERT_COOLDOWN):
                            self._mtf_monitoring[sym]["last_alert"] = now
                            tf_str = self._build_mtf_state_str(mtf_align["tfs"], scenario="monitoring")
                            await send_message(
                                f"📈 Monitoring Update — {sym}\n"
                                f"Root: {info['root']} | Price: {price}\n"
                                f"Flipped ✅: {', '.join(sorted(newly_flipped))}\n"
                                f"Still waiting: {', '.join(sorted(curr_neg))}\n"
                                f"MTF: {tf_str}"
                            )
            except Exception:
                logger.exception("Error checking monitored symbol %s", sym)

        for sym in to_remove:
            self._mtf_monitoring.pop(sym, None)

        if newly_aligned:
            await self.handle_root_signals(newly_aligned)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate MTF alignment for each root signal and act on each scenario.

        Scenario A  (aligned)       — all 5 MTF TFs positive → open trade if TRADE_ENABLED
        Scenario C  (daily_rising)  — only 1d negative but rising slope → open trade
        Scenario B  (monitoring)    — 1+ TFs negative → add to watch-list until last flip

        Returns the full evaluated list consumed by send_summary for block formatting.
        """
        evaluated: List[Dict[str, Any]] = []
        to_open:   List[Dict[str, Any]] = []

        for item in root_signals:
            sym        = item["symbol"]
            price      = item["price"]
            root       = item["root"]
            vol_change = item.get("vol_change")

            # Root-TF histogram value (compute if not pre-populated)
            hist = item.get("hist", [])
            if not hist:
                _, _, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            # Evaluate MTF alignment (Scenarios A / B / C)
            mtf_align    = self._compute_mtf_alignment(sym, price)
            mtf_status   = mtf_align["status"]
            negative_tfs = mtf_align.get("negative_tfs", [])

            # Composite score for ranking
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
                    tf_str = self._build_mtf_state_str(mtf_align["tfs"], scenario="monitoring")
                    await send_message(
                        f"⏳ Monitoring Started — {sym}\n"
                        f"Root: {root} | Price: {price}\n"
                        f"Waiting for TFs to flip: {', '.join(negative_tfs)}\n"
                        f"MTF: {tf_str}"
                    )

            evaluated.append(entry)

        await self._emit_event("candidates_evaluated", evaluated)

        # Apply ROOT_FILTER ranking to accepted candidates
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
            sym        = c["symbol"]
            price      = c["price"]
            vol_change = c.get("vol_change")
            reason_tag = c.get("reason", "signal")

            # ── Volume gate: blocks trade open only, never hides the signal ──
            if not self._passes_volume_gate(vol_change):
                vol_pct_str = f"{vol_change * 100:.1f}%" if vol_change is not None else "N/A"
                logger.info(
                    "VOLUME GATE BLOCKED trade open for %s — vol_change=%s (VOLUME_MIN_CHANGE_PCT=%.2f%%)",
                    sym, vol_pct_str, VOLUME_MIN_CHANGE_PCT * 100,
                )
                await send_message(
                    f"⛔ Trade blocked by volume gate — {sym}\n"
                    f"24h Vol Δ: {vol_pct_str} (requires > {VOLUME_MIN_CHANGE_PCT * 100:.1f}%)\n"
                    f"Signal remains active — waiting for volume to confirm."
                )
                continue

            # ── MACD histogram threshold check ────────────────────────────
            macd_hist_val = float(c.get("macd_hist_val") or 0.0)
            if macd_hist_val < MACD_HIST_THRESHOLD:
                logger.info(
                    "MACD THRESHOLD BLOCKED trade open for %s — hist_val=%.6f threshold=%.6f",
                    sym, macd_hist_val, MACD_HIST_THRESHOLD,
                )
                continue

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
            side = "Buy"

            # Strength for trade notice
            strength = self._signal_strength_rating(macd_hist_val, vol_change)
            vol_display = strength["vol_rating"]

            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    await send_message(
                        f"✅ *Trade Opened* — {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Strength: {strength['label']}\n"
                        f"  MACD Hist: {macd_hist_val:+.6f}\n"
                        f"  24h Vol Δ: {vol_display}\n"
                        f"Score: {c['score']:.2f} | Reason: {reason_tag}"
                    )
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
            else:
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"]})
                logger.info("Simulated open %s qty=%s score=%.2f", sym, qty, c["score"])
                await send_message(
                    f"📊 *Simulated Trade* — {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Strength: {strength['label']}\n"
                    f"  MACD Hist: {macd_hist_val:+.6f}\n"
                    f"  24h Vol Δ: {vol_display}\n"
                    f"Score: {c['score']:.2f} | Reason: {reason_tag}"
                )
            # Remove from monitoring watch-list if it was queued there
            self._mtf_monitoring.pop(sym, None)

        return evaluated

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None):
        """
        Send structured Telegram push notices on each scan interval.

        LAYOUT
        ──────
        Block 1  — «Bybit Perps» header block
                   Title, timestamp, per-TF signal counts, monitoring queue, open trades

        Block 2…N — One block per signal symbol, ordered:
                    • All 1h root signals first (sorted by signal strength desc)
                    • Then all 4h root signals
                    • Then all 1d root signals
                    Each block shows: symbol, price, MTF alignment / monitoring TFs,
                    signal-strength rating (MACD histogram + 24h vol Δ combined).

        Last Block — Recommendations: top picks from accepted signals up to MAX_OPEN_TRADES

        REPEAT LOGIC
        ────────────
        Signals are sent immediately on first detection.
        A signal for (symbol, tf) is re-sent only when its MTF alignment snapshot
        changes between scan intervals (e.g. last negative TF flips positive).
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())

        # ── Block 1: Bybit Perps header ─────────────────────────────────────
        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = sig.get("root", "?")
            tf_counts[rt] = tf_counts.get(rt, 0) + 1

        header_lines = [
            "📡 *Bybit Perps*",
            f"🕐 {now_str}",
        ]

        if root_signals:
            summary_parts = []
            for rt in ROOT_TFS:
                cnt = tf_counts.get(rt, 0)
                if cnt:
                    # Map numeric TF to human-readable
                    tf_label = {"60": "1h", "240": "4h", "D": "1d", "1h": "1h", "4h": "4h", "1d": "1d"}.get(rt, rt)
                    summary_parts.append(f"{tf_label} {cnt}")
            header_lines.append("Signals: " + " │ ".join(summary_parts) if summary_parts else "Signals: 0")
        else:
            header_lines.append("No new root signals this interval.")

        monitoring_count = len(self._mtf_monitoring)
        if monitoring_count:
            header_lines.append(f"⏳ Monitoring: {monitoring_count} pending MTF")

        open_sum = self.trade_manager.summary() if hasattr(self.trade_manager, "summary") else []
        if open_sum:
            header_lines.append(f"📂 Open trades: {len(open_sum)}/{MAX_OPEN_TRADES}")

        await send_message("\n".join(header_lines))

        if not root_signals:
            return

        # ── Build evaluation lookup keyed by (symbol, root) ─────────────────
        # Used as a cache: prefer pre-computed MTF from handle_root_signals when
        # available, but always fall back to a fresh _compute_mtf_alignment call
        # so the MTF status per block is NEVER left as "unknown".
        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        # ── Blocks 2…N: per-symbol signal blocks ────────────────────────────
        # Order: 1h → 4h → 1d  (ROOT_TFS order), within each TF sorted by strength desc
        signal_tfs = [rt for rt in ROOT_TFS if rt in tf_counts]
        accepted_signals: List[Dict[str, Any]] = []   # collect for recommendations block

        for rt in signal_tfs:
            tf_label = {"60": "1h", "240": "4h", "D": "1d", "1h": "1h", "4h": "4h", "1d": "1d"}.get(rt, rt)
            tf_sigs = [s for s in root_signals if s.get("root") == rt]

            # Attach strength rating to each sig for sorting
            enriched = []
            for sig in tf_sigs:
                sym        = sig["symbol"]
                ev         = eval_map.get((sym, rt), {})
                macd_val   = float(ev.get("macd_hist_val") or (sig.get("hist") or [0.0])[-1] if sig.get("hist") else 0.0)
                vol_change = sig.get("vol_change")
                strength   = self._signal_strength_rating(macd_val, vol_change)
                enriched.append((strength["score"], sig, ev, macd_val, vol_change, strength))

            enriched.sort(key=lambda x: x[0], reverse=True)

            for (_, sig, ev, macd_hist_val, vol_change, strength) in enriched:
                try:
                    sym   = sig["symbol"]
                    price = sig["price"]

                    # ── MTF alignment: always use a live evaluation ───────────
                    # eval_map may be empty (no root signals passed evaluated) or
                    # the entry may exist but have stale / missing mtf_status.
                    # Re-compute directly so the three Scenario rules always apply.
                    ev_mtf_status = ev.get("mtf_status", "")
                    if ev_mtf_status in ("aligned", "daily_rising", "monitoring"):
                        # Trust the pre-computed result from handle_root_signals
                        mtf_align    = {"status": ev_mtf_status, "tfs": ev.get("mtf", {}),
                                        "negative_tfs": ev.get("negative_tfs", []),
                                        "one_d_slope":  ev.get("one_d_slope")}
                    else:
                        # Fall back: compute fresh so State is never "❓ Unknown"
                        mtf_align = self._compute_mtf_alignment(sym, price)

                    mtf_status   = mtf_align["status"]           # "aligned" | "daily_rising" | "monitoring"
                    mtf_tfs_state = mtf_align["tfs"]             # per-TF state dicts
                    negative_tfs  = mtf_align.get("negative_tfs", [])
                    one_d_slope   = mtf_align.get("one_d_slope")

                    # Reflect fresh alignment back into ev so accepted_signals is accurate
                    ev_accept = mtf_status in ("aligned", "daily_rising")

                    # ── Dedup / repeat check ──────────────────────────────────
                    snap_key   = self._mtf_snapshot_key({"tfs": mtf_tfs_state}) if mtf_tfs_state else ""
                    signal_key = (sym, rt)
                    last_snap  = self._sent_signal_mtf.get(signal_key)

                    if last_snap is not None and last_snap == snap_key:
                        # MTF alignment unchanged — suppress repeat
                        logger.debug("Signal suppressed (no MTF change): %s %s", sym, rt)
                        if ev_accept:
                            accepted_signals.append((strength["score"], sym, rt, price, vol_change, strength, ev))
                        continue

                    # Update snapshot
                    self._sent_signal_mtf[signal_key] = snap_key

                    # ── Price formatting ──────────────────────────────────────
                    if price >= 1000:
                        price_str = f"${price:,.2f}"
                    elif price >= 1:
                        price_str = f"${price:.4f}"
                    else:
                        price_str = f"${price:.8f}"

                    # ── State line — strictly one of three scenario labels ─────
                    #   Scenario A → "✅ MTF Aligned"
                    #   Scenario C → "📈 Daily Rising (1d slope …)"
                    #   Scenario B → "⏳ Monitoring → waiting: <tfs>"
                    if mtf_status == "aligned":
                        align_str = "✅ MTF Aligned"
                    elif mtf_status == "daily_rising":
                        slope_note = f" (1d slope {one_d_slope:+.4f})" if one_d_slope is not None else ""
                        align_str = f"📈 Daily Rising{slope_note}"
                    else:  # "monitoring"
                        align_str = f"⏳ Monitoring → waiting: {', '.join(negative_tfs)}"

                    # ── MTF row — icons derived strictly from active scenario ──
                    mtf_str = self._build_mtf_state_str(mtf_tfs_state, scenario=mtf_status) if mtf_tfs_state else "N/A"

                    # ── Volume gate indicator (display only; never hides signal) ──
                    if vol_change is None:
                        vol_gate_icon = "⚪"
                    elif vol_change > VOLUME_MIN_CHANGE_PCT:
                        vol_gate_icon = "✅"
                    else:
                        vol_gate_icon = "🚫"

                    block_lines = [
                        f"📌 *Bybit Perp | {tf_label} Signal*",
                        f"Symbol: `{sym}`",
                        f"Price:  {price_str}",
                        f"",
                        f"Strength: {strength['label']}",
                        f"  MACD Hist: {macd_hist_val:+.6f}  ({strength['macd_rating']})",
                        f"  24h Vol Δ: {strength['vol_rating']} {vol_gate_icon}",
                        f"",
                        f"MTF: {mtf_str}",
                        f"State: {align_str}",
                    ]
                    await send_message("\n".join(block_lines))

                    if ev_accept:
                        accepted_signals.append((strength["score"], sym, rt, price, vol_change, strength, ev))

                except Exception:
                    logger.exception("Failed to send signal block for %s %s", sig.get("symbol"), rt)

        # ── Last Block: Recommendations ──────────────────────────────────────
        if not accepted_signals:
            return

        accepted_signals.sort(key=lambda x: x[0], reverse=True)
        top_n = accepted_signals[:MAX_OPEN_TRADES]

        rec_lines = [
            f"🏆 *Top Picks* (best {len(top_n)} of {len(accepted_signals)} aligned signals)",
            f"Max open trades: {MAX_OPEN_TRADES}",
            "",
        ]
        for rank, (score, sym, rt, price, vol_change, strength, ev) in enumerate(top_n, 1):
            tf_label = {"60": "1h", "240": "4h", "D": "1d", "1h": "1h", "4h": "4h", "1d": "1d"}.get(rt, rt)
            if price >= 1000:
                price_str = f"${price:,.2f}"
            elif price >= 1:
                price_str = f"${price:.4f}"
            else:
                price_str = f"${price:.8f}"
            vol_pass = self._passes_volume_gate(vol_change)
            gate_note = "" if vol_pass else " ⛔ vol gate"
            rec_lines.append(f"{rank}. {sym} ({tf_label}) — {price_str} {strength['label']}{gate_note}")

        await send_message("\n".join(rec_lines))

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
