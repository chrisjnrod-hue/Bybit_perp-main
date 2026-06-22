# scanner.py - FINAL PRODUCTION VERSION (modified for dedupe fixes and Telegram format)
# Last Updated: 2026-06-22
import os
import asyncio
import time
import json
from collections import defaultdict, deque
from typing import Dict, List, Any, Optional, Callable, Tuple
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
    REST_POLL_INTERVAL,
    # new signal config
    SIGNAL_FILTER_MACD_ENABLED, SIGNAL_FILTER_VOLUME_ENABLED, SIGNAL_FILTER_SR_ENABLED,
    SIGNAL_WEIGHT_MACD, SIGNAL_WEIGHT_VOLUME, SIGNAL_WEIGHT_SR,
    SIGNAL_SR_SUPPORT_WINDOW_PCT, SIGNAL_SR_LOOKBACK, SENT_SIGNAL_TTL
)
from .telegram import send_message
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket

getcontext().prec = 28
logger = get_logger("scanner")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# ============ MTF Alignment TFs — NUMERIC format matching Bybit API ============
MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
ROOT_ORDER = ["60", "240", "D"]  # ordering for per-TF listing (1h, 4h, 1d)


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

        # New: track sent signals to avoid duplicate telegram posts per same candle
        # key: (symbol, root, candle_start) -> timestamp_sent
        self._sent_signals: Dict[Tuple[str, str, Optional[int]], float] = {}

        # New: last processed root candle start per TF
        self._last_root_candle_start: Dict[str, Optional[int]] = {tf: None for tf in ROOT_TFS}

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s)",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE
        )

    # ---------------- Helper: aligned candle start ----------------
    def _aligned_candle_start(self, root: str, kline_start: Optional[int] = None) -> Optional[int]:
        """
        Return a deterministic candle_start integer for deduping:
          - Prefer provided kline_start when valid
          - Otherwise align current time to the root TF boundary
        """
        try:
            if kline_start and isinstance(kline_start, (int, float)):
                return int(kline_start)
            sec = self._tf_to_seconds(root)
            if sec and sec > 0:
                return int(time.time()) // sec * sec
        except Exception:
            pass
        return None

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

            # Subscribe to correct TF format (numeric, not "5m" format)
            if USE_WS and syms:
                tasks = []
                sem = asyncio.Semaphore(max(1, CONCURRENCY))
                tfs_to_sub = list(set(list(ROOT_TFS) + ["5", "15"]))

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

                # All WS subscriptions are fire-and-forget, gather all at once
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    logger.info("[DIAGNOSTIC] WS subscriptions complete for %d subscriptions", len(tasks))

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
        """
        FIX #21: Properly normalize kline responses handling dicts, lists, and tuples
        Extended to capture open/high/low when available (required for SR detection).
        """
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

        # Ensure we have a list/tuple before iterating (FIX #21)
        if not isinstance(raw_klines, (list, tuple)):
            if isinstance(raw_klines, dict):
                # If we got a single dict response, wrap it
                seq = [raw_klines]
            else:
                seq = [raw_klines] if raw_klines else []
        else:
            seq = raw_klines

        for item in seq:
            try:
                if isinstance(item, (list, tuple)):
                    start = None
                    open_p = None
                    high = None
                    low = None
                    close = None
                    vol = None
                    if len(item) >= 1:
                        try:
                            start = int(item[0])
                        except Exception:
                            start = None
                    if len(item) >= 2:
                        try:
                            open_p = float(item[1])
                        except Exception:
                            open_p = None
                    if len(item) >= 3:
                        try:
                            high = float(item[2])
                        except Exception:
                            high = None
                    if len(item) >= 4:
                        try:
                            low = float(item[3])
                        except Exception:
                            low = None
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
                        out.append({"start_at": start, "open": open_p, "high": high, "low": low, "close": close, "volume": vol})
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
                    open_p = (
                        item.get("open")
                        or item.get("o")
                    )
                    high = (
                        item.get("high")
                        or item.get("h")
                    )
                    low = (
                        item.get("low")
                        or item.get("l")
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
                        if open_p is not None:
                            open_p = float(open_p)
                    except Exception:
                        open_p = None
                    try:
                        if high is not None:
                            high = float(high)
                    except Exception:
                        high = None
                    try:
                        if low is not None:
                            low = float(low)
                    except Exception:
                        low = None
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
                        out.append({"start_at": start, "open": open_p, "high": high, "low": low, "close": close, "volume": vol, "is_closed": is_closed})
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
                            valid.append({"start_at": start, "open": c.get("open"), "high": c.get("high"), "low": c.get("low"), "close": float(close), "volume": c.get("volume")})
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
                    # FIX #11: Semaphore per TF, not per symbol to respect concurrent limits
                    tfs_to_poll = list(set(ROOT_TFS + ["5", "15"]))
                    for tf in tfs_to_poll:
                        try:
                            async with self.request_sem:
                                data = await self._call_get_klines(sym, tf, limit=3)
                                normalized = self._normalize_klines(data, tf) if data else []

                                if normalized:
                                    lst = self.kline_store.get(sym, {}).get(tf, [])
                                    last_new = None

                                    for c in reversed(normalized):
                                        if c.get("close") is not None:
                                            last_new = {
                                                "start_at": c.get("start_at"),
                                                "open": c.get("open"),
                                                "high": c.get("high"),
                                                "low": c.get("low"),
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

                    # FIX #23: Removed timeout - let tasks complete naturally, return exceptions
                    await asyncio.gather(*tasks, return_exceptions=True)

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
                    await asyncio.sleep(0.1)  # Give it time to cancel
                except Exception:
                    pass
                self._rest_poller_task = None
            return

        if self._rest_poller_task and not self._rest_poller_task.done():
            return

        logger.info("[REST_POLLER_START] WS unavailable, starting REST poller as fallback")
        self._rest_poller_task = asyncio.create_task(self._rest_poller())

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        """
        FIX #24: Consistent MACD calculation - always replace last close with current price
        """
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

        # Determine current price (consistent logic - FIX #24)
        current_price = None
        if include_price is not None:
            current_price = float(include_price)
        elif use_ws_current and USE_WS and hasattr(self.client, "get_ws_latest_kline"):
            try:
                ws_last = self.client.get_ws_latest_kline(symbol, tf)
                if ws_last and ws_last.get("close") is not None:
                    current_price = float(ws_last.get("close"))
            except Exception:
                pass

        # Consistently replace last close with current price (FIX #24)
        if current_price is not None:
            if closes:
                closes[-1] = current_price
            else:
                closes.append(current_price)

        macd_line, signal_line, hist = macd_histogram(closes)
        if DEBUG_SURGICAL_LOGS:
            valid_hist_count = sum(1 for h in hist if h is not None) if hist else 0
            logger.info("[SURGICAL_LOG_3] MACD_CALC %s %s: closes_count=%d, hist_length=%d, valid_hist=%d, last_hist=%s",
                       symbol, tf, len(closes), len(hist) if hist else 0, valid_hist_count, hist[-1] if hist and len(hist) > 0 else None)

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

    def compute_sr_levels(self, symbol: str, tf: str, lookback: Optional[int] = None) -> Dict[str, Optional[float]]:
        """
        Compute a simple SR hybrid from recent candles:
          - find local swing highs/lows
          - compute pivot (P) and R1/S1
          - return nearest support (below price) and resistance (above price) and pct distances
        """
        try:
            lookback = int(lookback or SIGNAL_SR_LOOKBACK or 100)
        except Exception:
            lookback = 100
        data = self.kline_store.get(symbol, {}).get(tf, [])
        if not data:
            return {"support": None, "resistance": None, "support_dist_pct": None, "resistance_dist_pct": None, "levels": []}

        # Work on last N candles
        seq = data[-lookback:] if len(data) >= lookback else list(data)
        highs = []
        lows = []
        closes = []
        for c in seq:
            closes.append(c.get("close"))
            highs.append(c.get("high") if c.get("high") is not None else c.get("close"))
            lows.append(c.get("low") if c.get("low") is not None else c.get("close"))

        levels: List[float] = []

        # local swing highs/lows detection (simple)
        for i in range(1, len(seq)-1):
            try:
                h = highs[i]
                if h is not None and highs[i-1] is not None and highs[i+1] is not None and h > highs[i-1] and h > highs[i+1]:
                    levels.append(float(h))
                l = lows[i]
                if l is not None and lows[i-1] is not None and lows[i+1] is not None and l < lows[i-1] and l < lows[i+1]:
                    levels.append(float(l))
            except Exception:
                continue

        # Last candle pivot (classic) - only if we have at least 2 candles
        try:
            if len(seq) >= 2:
                last_h = highs[-1]
                last_l = lows[-1]
                last_c = closes[-1]
                if last_h is not None and last_l is not None and last_c is not None:
                    P = (last_h + last_l + last_c) / 3.0
                    R1 = 2 * P - last_l
                    S1 = 2 * P - last_h
                    levels.extend([P, R1, S1])
        except Exception:
            pass

        # Deduplicate close values
        levels = sorted(set([float(v) for v in levels if v is not None and v > 0]))

        # current price best guess
        price = None
        try:
            price = float(self._last_price_cache.get(symbol) or seq[-1].get("close"))
        except Exception:
            price = None

        if not price:
            return {"support": None, "resistance": None, "support_dist_pct": None, "resistance_dist_pct": None, "levels": levels}

        support = None
        resistance = None
        # find nearest support below price and nearest resistance above
        for lvl in reversed(levels):
            if lvl < price:
                support = lvl
                break
        for lvl in levels:
            if lvl > price:
                resistance = lvl
                break

        support_dist = None
        resistance_dist = None
        try:
            if support is not None:
                support_dist = (price - support) / price
            if resistance is not None:
                resistance_dist = (resistance - price) / price
        except Exception:
            pass

        return {"support": support, "resistance": resistance, "support_dist_pct": support_dist, "resistance_dist_pct": resistance_dist, "levels": levels}

    async def send_signal_block(self, sig: Dict[str, Any], ev: Dict[str, Any]):
        """
        Send single Telegram block in older-screenshot format:
        📌 Bybit Perp | {rt} Signal
        Symbol: ...
        Price: $...
        MACD H: +0.000004
        24h Vol Δ: +2.3% / N/A
        MTF State: 5❌ 15❌ 60❌ 240✅ D🔄
        Status: ⏳ Monitoring (Not accepted yet)
        Score: 5.50
        """
        try:
            sym = sig.get("symbol") or ev.get("symbol")
            rt = sig.get("root") or ev.get("root")
            price = sig.get("price") if sig.get("price") is not None else ev.get("price")
            # safe price formatting
            try:
                p = float(price) if price is not None else None
            except Exception:
                p = None
            if p is None:
                price_str = "N/A"
            else:
                if p >= 1000:
                    price_str = f"${p:,.2f}"
                elif p >= 1:
                    price_str = f"${p:.4f}"
                else:
                    price_str = f"${p:.8f}"

            macd_hist_val = float(ev.get("macd_hist_val") or 0.0)
            vol_change = ev.get("vol_change")
            vol_str = "N/A"
            if vol_change is not None:
                try:
                    vol_str = f"{vol_change * 100:+.1f}%"
                except Exception:
                    vol_str = str(vol_change)

            mtf_tfs = ev.get("mtf", {})
            mtf_state_str = self._build_mtf_state_str(mtf_tfs) if mtf_tfs else "N/A"
            mtf_status = ev.get("mtf_status", "unknown")
            if mtf_status == "aligned":
                status_str = "✅ Aligned (Accept)"
            elif mtf_status == "daily_rising":
                status_str = "📈 Daily Rising (Accept)"
            elif mtf_status == "monitoring":
                status_str = "⏳ Monitoring (Not accepted yet)"
            else:
                status_str = "❓ Unknown"

            score = float(ev.get("score") or 0.0)

            block_lines = [
                f"📌 Bybit Perp | {rt} Signal",
                f"Symbol: {sym}",
                f"Price: {price_str}",
                f"MACD H: {macd_hist_val:+.6f}",
                f"24h Vol Δ: {vol_str}",
                f"MTF State: {mtf_state_str}",
                f"Status: {status_str}",
                f"Score: {score:.2f}"
            ]
            await send_message("\n".join(block_lines))
        except Exception:
            logger.exception("Failed to send single signal block for %s", sig.get("symbol"))

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
                _, _, hist = self.compute_macd_for(sym, root, include_price=price)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            mtf_align    = self._compute_mtf_alignment(sym, price)
            mtf_status   = mtf_align["status"]
            negative_tfs = mtf_align.get("negative_tfs", [])

            # base scoring derived from MTF positives/flips (existing)
            score  = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive"))
            score += sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))

            # volume contribution (existing vol_change added + new scoring behavior)
            vol_score = 0.0
            if vol_change is not None:
                try:
                    score += min(vol_change, 1.0)
                except Exception:
                    pass
                if SIGNAL_FILTER_VOLUME_ENABLED:
                    if vol_change > 0:
                        vol_score = min(vol_change, 1.0) * SIGNAL_WEIGHT_VOLUME
                    else:
                        vol_score = -0.5 * min(abs(vol_change), 1.0) * SIGNAL_WEIGHT_VOLUME
                    score += vol_score

            # MACD strength scoring
            macd_score = 0.0
            try:
                if SIGNAL_FILTER_MACD_ENABLED:
                    # contribution proportional to magnitude, capped
                    macd_score = min(abs(macd_hist_val) / (abs(macd_hist_val) + 1.0), 2.0) * SIGNAL_WEIGHT_MACD
                    score += macd_score
            except Exception:
                macd_score = 0.0

            # SR proximity scoring
            sr_info = {}
            sr_score = 0.0
            if SIGNAL_FILTER_SR_ENABLED:
                try:
                    sr_info = self.compute_sr_levels(sym, root)
                    sdist = sr_info.get("support_dist_pct")
                    rdist = sr_info.get("resistance_dist_pct")
                    near_pct = SIGNAL_SR_SUPPORT_WINDOW_PCT or 0.02
                    if sdist is not None and sdist >= 0 and sdist <= near_pct:
                        sr_score = SIGNAL_WEIGHT_SR * (1.0 - (sdist / (near_pct + 1e-9)))
                    elif rdist is not None and rdist >= 0 and rdist <= near_pct:
                        sr_score = -SIGNAL_WEIGHT_SR * (1.0 - (rdist / (near_pct + 1e-9)))
                    else:
                        sr_score = 0.0
                    score += sr_score
                except Exception:
                    sr_info = {}

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
                "score_breakdown": {"macd": macd_score, "vol": vol_score, "sr": sr_score},
                "sr": sr_info,
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
                # Note: we still send individual simulated trade messages as before for transparency
                await send_message(
                    f"📊 Simulated Trade — {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Score: {c['score']:.2f} | Reason: {reason_tag}"
                )
            self._mtf_monitoring.pop(sym, None)

        return evaluated

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None):
        """
        Revised Telegram summary to match requested layout:

        1) Single block with recommended trade opens (up to available slots)
        2) Single block root TF summary + full signals listing (all root TF signals)
        3) One block per signal, grouped in order: 1h (60), 4h (240), 1d (D).
        Each signal block formatted like the screenshot: Price, MACD H, 24h Vol Δ (only value), MTF State, Status, Score
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())

        evaluated = evaluated or []
        # build dict for quick lookup
        eval_dict = {(e["symbol"], e["root"]): e for e in evaluated}

        # Build overall signals list for the root_signals param
        # root_signals is a list of dicts with keys symbol, root, price, hist, vol_change, candle_start
        # First block: trade recommendations (based on available slots)
        try:
            current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
            slots = max(0, MAX_OPEN_TRADES - current_open)
            # collect accepted signals
            accepted = [e for e in evaluated if e.get("accept")]
            accepted_sorted = sorted(accepted, key=lambda r: r.get("score", 0.0), reverse=True)
            recommended = accepted_sorted[:slots] if slots > 0 else []

            if recommended:
                rec_lines = [f"📣 Recommended Signals for Trading — {now_str}"]
                rec_lines.append(f"Open trades: {current_open} / {MAX_OPEN_TRADES}")
                rec_lines.append(f"Slots available: {slots}")
                for r in recommended:
                    sym = r["symbol"]
                    rt = r["root"]
                    price = r.get("price")
                    try:
                        p = float(price) if price is not None else None
                    except Exception:
                        p = None
                    if p is None:
                        price_str = "N/A"
                    else:
                        if p >= 1000:
                            price_str = f"${p:,.2f}"
                        elif p >= 1:
                            price_str = f"${p:.4f}"
                        else:
                            price_str = f"${p:.8f}"
                    score = float(r.get("score", 0.0))
                    rec_lines.append(f" - {sym} | {rt} | {price_str} | score={score:.2f}")
                await send_message("\n".join(rec_lines))
        except Exception:
            logger.exception("Failed to send recommended signals block")

        # Second block: Root TF summary + full listing in one block
        try:
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
                        header_lines.append(f" {rt}: {cnt} (window: {win})")
                    else:
                        header_lines.append(f" {rt}: {cnt}")

            header_lines.append("")
            header_lines.append("Signals (all root TF signals):")
            if not root_signals:
                header_lines.append(" - None")
            else:
                # print each in one line: - SYMBOL | ROOT | $PRICE
                for sig in root_signals:
                    sym = sig.get("symbol")
                    rt = sig.get("root")
                    price = sig.get("price", 0)
                    try:
                        p = float(price) if price is not None else None
                    except Exception:
                        p = None
                    if p is None:
                        price_str = "N/A"
                    else:
                        if p >= 1000:
                            price_str = f"${p:,.2f}"
                        elif p >= 1:
                            price_str = f"${p:.8f}"
                        else:
                            price_str = f"${p:.8f}"
                    header_lines.append(f" - {sym} | {rt} | {price_str}")
            await send_message("\n".join(header_lines))
        except Exception:
            logger.exception("Failed to send root TF summary block")

        # Then send one block per signal grouped by TF order 60 -> 240 -> D
        try:
            # group signals by TF
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for sig in root_signals:
                grouped.setdefault(sig.get("root"), []).append(sig)

            for rt in ROOT_ORDER:
                lst = grouped.get(rt, [])
                if not lst:
                    continue
                # for each signal in this tf, send single block
                for s in lst:
                    key = (s.get("symbol"), s.get("root"))
                    ev = eval_dict.get(key, {})
                    # ensure ev has macd_hist_val and score; if not, create minimal
                    if not ev:
                        ev = {"symbol": s.get("symbol"), "root": s.get("root"), "price": s.get("price"), "macd_hist_val": (s.get("hist") or [])[-1] if s.get("hist") else 0.0, "score": 0.0, "vol_change": s.get("vol_change"), "mtf": {}}
                    await self.send_signal_block(s, ev)
        except Exception:
            logger.exception("Failed to send per-signal blocks in grouped order")

    def _compute_mtf_alignment(self, symbol: str, price: float) -> Dict[str, Any]:
        """
        Evaluate MTF alignment across MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
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
            _, _, hist = self.compute_macd_for(symbol, tf, include_price=price)
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

                # determine whether any root TF candle has just opened (global per-TF check)
                now_ts = int(time.time())
                new_root_candle_tfs = set()
                for rt in ROOT_TFS:
                    try:
                        sec = self._tf_to_seconds(rt)
                        if sec <= 0:
                            continue
                        # aligned start timestamp for the current candle
                        current_candle_start = int(now_ts // sec * sec)
                        last_known = self._last_root_candle_start.get(rt)
                        if last_known is None:
                            # initialize but do not treat as new
                            self._last_root_candle_start[rt] = current_candle_start
                        elif current_candle_start != last_known:
                            # new candle has started for this TF
                            new_root_candle_tfs.add(rt)
                            self._last_root_candle_start[rt] = current_candle_start
                    except Exception:
                        continue

                async def check_symbol(sym: str):
                    try:
                        async with self.request_sem:
                            price = await self.client.get_latest_price(sym)

                        if price is None:
                            try:
                                if ROOT_TFS and USE_WS and self.client.is_ws_connected():
                                    ws_last = self.client.get_ws_latest_kline(sym, ROOT_TFS[0]) if hasattr(self.client, "get_ws_latest_kline") else None
                                    if ws_last and ws_last.get("close") is not None:
                                        price = float(ws_last.get("close"))
                            except Exception:
                                price = None

                        if price is None:
                            return

                        self._last_price_cache[sym] = price

                        # Optimize volume caching - only update every 5 symbols to save API calls
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

                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                candle_start = None
                                try:
                                    ks = self.kline_store.get(sym, {}).get(root, [])
                                    candle_start = ks[-1].get("start_at") if ks else None
                                except Exception:
                                    candle_start = None
                                # Ensure a deterministic candle_start for dedupe
                                aligned_start = self._aligned_candle_start(root, candle_start)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change,
                                    "candle_start": aligned_start
                                })
                                logger.info("SIGNAL DETECTED: %s %s @ %s", sym, root, price)
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

                await self._emit_event("root_signals", root_signals)

                await self._check_monitored_symbols()

                # Evaluate signals and compute scores (opens happen here as before)
                evaluated = []
                if root_signals:
                    evaluated = await self.handle_root_signals(root_signals)
                else:
                    evaluated = []

                # Decide send behavior:
                # - If a root TF candle opened this cycle, send full summary for TF(s) per rules:
                #    * when 60 opens -> send 1h summary
                #    * when 240 opens -> send 1h + 4h
                #    * when D opens -> send 1h + 4h + D
                # - Else: send only single-signal blocks for newly discovered signals (deduped)
                send_full_tfs = set()
                if "60" in new_root_candle_tfs:
                    send_full_tfs.add("60")
                if "240" in new_root_candle_tfs:
                    send_full_tfs.update({"60", "240"})
                if "D" in new_root_candle_tfs:
                    send_full_tfs.update({"60", "240", "D"})

                if send_full_tfs:
                    # prepare filtered lists per TF
                    tf_filtered_signals = [s for s in root_signals if s.get("root") in send_full_tfs]
                    # evaluate map for these signals
                    eval_for_send = []
                    eval_dict = {(e["symbol"], e["root"]): e for e in evaluated}
                    for s in tf_filtered_signals:
                        e = eval_dict.get((s["symbol"], s["root"]))
                        if e:
                            eval_for_send.append(e)
                        else:
                            # fallback minimal
                            eval_for_send.append({"symbol": s.get("symbol"), "root": s.get("root"), "price": s.get("price"), "macd_hist_val": (s.get("hist") or [])[-1] if s.get("hist") else 0.0, "score": 0.0, "vol_change": s.get("vol_change"), "mtf": {}})

                    logger.info("Sending full summary for TFs=%s signals=%d", send_full_tfs, len(tf_filtered_signals))
                    try:
                        # send in the requested order: first recommended block, then root summary+listing, then per-signal blocks grouped
                        await self.send_summary(tf_filtered_signals, eval_for_send)
                        # mark these signals as sent (so we don't resend per-signal blocks)
                        ts_now = time.time()
                        for s in tf_filtered_signals:
                            key = (s["symbol"], s["root"], s.get("candle_start"))
                            aligned = self._aligned_candle_start(s["root"], s.get("candle_start"))
                            key = (s["symbol"], s["root"], aligned)
                            self._sent_signals[key] = ts_now
                    except Exception:
                        logger.exception("Failed to send full summary for TFs=%s", send_full_tfs)
                else:
                    # non-candle-open cycle: send per-signal single blocks for newly detected signals
                    if root_signals:
                        ts_now = time.time()
                        # build evaluated map for lookups
                        eval_map = {(e["symbol"], e["root"]): e for e in evaluated}
                        for s in root_signals:
                            aligned = self._aligned_candle_start(s["root"], s.get("candle_start"))
                            key = (s["symbol"], s["root"], aligned)
                            prev = self._sent_signals.get(key)
                            if prev and (time.time() - prev) < SENT_SIGNAL_TTL:
                                # already sent recently
                                continue
                            # find evaluated entry if available
                            matching = eval_map.get((s["symbol"], s["root"]))
                            if not matching:
                                matching = {"symbol": s.get("symbol"), "root": s.get("root"), "price": s.get("price"), "macd_hist_val": (s.get("hist") or [])[-1] if s.get("hist") else 0.0, "score": 0.0, "vol_change": s.get("vol_change"), "mtf": {}}
                            await self.send_signal_block(s, matching)
                            self._sent_signals[key] = ts_now

                # cleanup old sent_signals
                try:
                    nowt = time.time()
                    to_delete = []
                    for k, v in list(self._sent_signals.items()):
                        if nowt - v > SENT_SIGNAL_TTL:
                            to_delete.append(k)
                    for k in to_delete:
                        self._sent_signals.pop(k, None)
                except Exception:
                    logger.exception("sent_signals cleanup failed")

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start

            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                # FIX #17: Align to actual Bybit 5m candle boundaries (0, 5, 10, 15... minutes UTC)
                now = time.time()
                now_struct = time.gmtime(now)
                current_minute = now_struct.tm_min
                current_second = now_struct.tm_sec

                # Find next 5-minute boundary
                next_5m_minute = ((current_minute // 5) + 1) * 5

                if next_5m_minute >= 60:
                    # Move to next hour
                    to_sleep = (60 - current_minute) * 60 - current_second
                else:
                    # Sleep to next 5m boundary
                    to_sleep = ((next_5m_minute - current_minute) * 60) - current_second

                to_sleep = max(0, to_sleep)
                logger.debug("[DIAGNOSTIC] Aligning to next 5m candle: sleeping %.1f seconds", to_sleep)
                await asyncio.sleep(to_sleep)

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
