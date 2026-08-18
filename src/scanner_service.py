# scanner_service.py
# Core scanning, signal evaluation, and trade management orchestration.
# Includes diagnostics and fast-refresh to enable immediate scans at the boundary.

import os
import asyncio
import time
import json
from collections import defaultdict
from typing import Dict, List, Any, Optional, Callable, Tuple
import math
import inspect
from decimal import getcontext

getcontext().prec = 28

from .logger import get_logger
from .bybit_client import BybitClient
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket
from .macd import slope  # used in a couple places still
from .config import (
    EXCLUDE_STABLECOINS, CONCURRENCY, KLINE_SEED_LIMIT,
    ROOT_TFS, MTF_TFS, ROOT_SCAN_INTERVAL, TRADE_ENABLED,
    MTF_SLOPE_LOOKBACK, ROOT_FILTER, ROOT_TOP_N, MAX_OPEN_TRADES, USE_WS,
    MAX_CONCURRENT_REQUESTS, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY,
    REST_POLL_INTERVAL, VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT, TECHNICAL_RATING,
    FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW, TRADE_RATING_MIN, TRADE_RATING_PRIORITIZE
)
from .telegram import send_message

# Delay importing scanner_core at module import time to avoid circular import issues.
def _import_scanner_core():
    try:
        from . import scanner_core
        return scanner_core
    except Exception:
        import importlib
        pkg = __package__ or ""
        modname = f"{pkg}.scanner_core" if pkg else "scanner_core"
        return importlib.import_module(modname)


from .scanner_telegram import TelegramSummary

logger = get_logger("scanner")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# Numeric TV rating threshold
try:
    TRADE_RATING_MIN_VAL = float(os.getenv("TRADE_RATING_MIN", str(TRADE_RATING_MIN)))
except (ValueError, TypeError):
    TRADE_RATING_MIN_VAL = TRADE_RATING_MIN

try:
    TV_RATING_WEIGHT = float(os.getenv("TV_RATING_WEIGHT", "0.3"))
    TV_RATING_WEIGHT = max(0.0, min(1.0, TV_RATING_WEIGHT))
except (ValueError, TypeError):
    TV_RATING_WEIGHT = 0.3

TRADE_NO_NEG_VOL = os.getenv("TRADE_NO_NEG_VOL", "1").strip().lower() in ("1", "true", "yes", "y")
MARKET_CAP_MIN = float(os.getenv("MARKET_CAP_MIN", "0") or 0)
PRIORITIZE_SLOT_ORDER = [p.strip() for p in os.getenv("PRIORITIZE_SLOT_ORDER", "240,D,60").split(",") if p.strip()]

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
TELEGRAM_DISPATCH_WINDOW = 5

DEFAULT_TRADE_MANAGER_CONFIG = {
    "STATE_FILE": os.getenv("TRADE_STATE_FILE", "open_trades.json"),
    "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", ""),
    "MAX_OPEN_TRADES": MAX_OPEN_TRADES,
    "MIN_MARKET_CAP": float(os.getenv("MIN_MARKET_CAP", "50000000")),
    "MAX_SPREAD_PERCENT": float(os.getenv("MAX_SPREAD_PERCENT", "0.1")),
    "MAX_SLIPPAGE": float(os.getenv("MAX_SLIPPAGE", "0.2")),
    "LEVERAGE": int(os.getenv("LEVERAGE", "10")),
    "TP_PERCENT": float(os.getenv("TP_PERCENT", "2.0")),
    "SL_PERCENT": float(os.getenv("SL_PERCENT", "1.0")),
    "BREAKEVEN_TRIGGER_PERCENT": float(os.getenv("BREAKEVEN_TRIGGER_PERCENT", "0.5")),
    "BREAKEVEN_HIGHER_LOWS": os.getenv("BREAKEVEN_HIGHER_LOWS", "1").strip().lower() in ("1", "true", "yes", "y"),
}


class Scanner:
    def __init__(self):
        self.rate_limiter = TokenBucket(max(1.0, float(1)))
        self.client = BybitClient(rate_limiter=self.rate_limiter)
        self.trade_manager = TradeManager(exchange_client=self.client, config=DEFAULT_TRADE_MANAGER_CONFIG)
        self.concurrent_sem = asyncio.Semaphore(max(1, CONCURRENCY))
        self.request_sem = asyncio.Semaphore(max(1, MAX_CONCURRENT_REQUESTS))
        self.kline_store: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(dict)
        self.symbols: List[str] = []
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._rest_poller_task: Optional[asyncio.Task] = None
        self._seeding_task: Optional[asyncio.Task] = None
        self._callbacks: List[Callable[[str, Any], Any]] = []
        self._24h_volumes: Dict[str, Dict[str, float]] = {}
        self._last_price_cache: Dict[str, float] = {}
        self._last_price_time: Dict[str, float] = {}
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        self._symbol_check_count = 0

        self._last_tf_candle_open_times: Dict[str, float] = {tf: 0.0 for tf in ROOT_TFS}
        self._last_telegram_dispatch_time: Optional[float] = None

        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        self.telegram = TelegramSummary()

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) "
            "TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_RATING_PRIORITIZE=%s FLIP_CANDLE_AGE_MAX_SEC=%d SIGNAL_DEDUP_WINDOW=%d "
            "TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN_VAL, TV_RATING_WEIGHT, TRADE_RATING_PRIORITIZE, FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW,
            TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER
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

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    logger.info("[DIAGNOSTIC] WS subscriptions complete for %d subscriptions", len(tasks))

            await self._ensure_rest_poller()
            logger.info("[DIAGNOSTIC] discover_symbols: COMPLETE - ready to scan")
            return syms
        except Exception:
            logger.exception("discover_symbols failed")
            return []

    async def _call_get_klines(self, symbol: str, tf: str, limit: int):
        names = ["get_klines", "getKlines", "get_klines_v2", "get_kline", "getKline"]
        return await self._call_client_method(names, symbol, tf, limit)

    async def seed_klines_for_symbol(self, symbol: str):
        core = _import_scanner_core()
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

                normalized = core.normalize_klines(raw, tf)

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
                            valid.append({
                                "start_at": start,
                                "open": c.get("open"),
                                "high": c.get("high"),
                                "low": c.get("low"),
                                "close": float(close),
                                "volume": c.get("volume")
                            })
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

    async def _fast_refresh_latest(self, symbols: List[str], tfs: List[str], limit: int = 2):
        """
        Fast refresh: fetch only the last 'limit' klines for given tfs and update self.kline_store
        with the most recent candle. This is used at scan boundary to ensure scans run immediately
        using the latest candle while a full seed runs in background.
        """
        logger.info("[REFRESH_FAST] Starting fast refresh for %d symbols x %d tfs", len(symbols), len(tfs))
        core = _import_scanner_core()
        async def worker(sym: str):
            for tf in tfs:
                try:
                    async with self.request_sem:
                        raw = await self._call_get_klines(sym, tf, limit=limit)
                    if not raw:
                        continue
                    normalized = core.normalize_klines(raw, tf)
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
                        try:
                            lst = self.kline_store.get(sym, {}).get(tf, [])
                            if lst:
                                if lst[-1].get("start_at") == last_new.get("start_at"):
                                    lst[-1] = last_new
                                else:
                                    lst.append(last_new)
                                self.kline_store[sym][tf] = lst
                            else:
                                self.kline_store.setdefault(sym, {})[tf] = [last_new]
                        except Exception:
                            self.kline_store.setdefault(sym, {})[tf] = [last_new]
                except Exception:
                    logger.debug("Fast refresh failed for %s %s", sym, tf, exc_info=True)

        # run workers in batches to avoid overloading
        for i in range(0, len(symbols), REQUEST_BATCH_SIZE):
            batch = symbols[i:i + REQUEST_BATCH_SIZE]
            tasks = [asyncio.create_task(worker(s)) for s in batch]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if i + REQUEST_BATCH_SIZE < len(symbols):
                await asyncio.sleep(REQUEST_BATCH_DELAY)
        logger.info("[REFRESH_FAST] Completed fast refresh")

    async def _rest_poller(self):
        """REST poller as fallback when WS is unavailable."""
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
                    tfs_to_poll = list(set(ROOT_TFS + ["5", "15"]))
                    for tf in tfs_to_poll:
                        try:
                            async with self.request_sem:
                                data = await self._call_get_klines(sym, tf, limit=3)
                                core = _import_scanner_core()
                                normalized = core.normalize_klines(data, tf) if data else []

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
                        except Exception:
                            logger.debug("REST poll kline failed for %s %s", sym, tf, exc_info=True)

                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(poll_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks, return_exceptions=True)

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
        """Ensure REST poller is running as fallback when WS is unavailable."""
        if USE_WS and self.client.is_ws_connected():
            if self._rest_poller_task and not self._rest_poller_task.done():
                try:
                    self._rest_poller_task.cancel()
                    await asyncio.sleep(0.1)
                except Exception:
                    pass
                self._rest_poller_task = None
            return

        if self._rest_poller_task and not self._rest_poller_task.done():
            return

        logger.info("[REST_POLLER_START] WS unavailable, starting REST poller as fallback")
        self._rest_poller_task = asyncio.create_task(self._rest_poller())

    # wrapper helpers that use scanner_core lazily
    def _tf_to_seconds(self, tf: str) -> int:
        core = _import_scanner_core()
        return core.tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str):
        core = _import_scanner_core()
        return core.normalize_klines(raw_klines, tf)

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        core = _import_scanner_core()
        return core.quantize_qty(qty, step, min_qty)

    def _get_current_candle_start(self, tf: str, now: float) -> int:
        tf_sec = self._tf_to_seconds(tf)
        if tf == "D":
            return int(now // 86400) * 86400
        elif tf == "W":
            return int(now // 604800) * 604800
        else:
            return int(now // tf_sec) * tf_sec

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        core = _import_scanner_core()
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

        return core.compute_macd_from_closes(closes, include_price=current_price)

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        """
        Diagnostic wrapper for core.detect_flip_current_open.
        """
        try:
            hist_len = len(hist) if hist else 0
            last3 = hist[-3:] if hist_len >= 3 else (hist[:] if hist else [])
            prev = None
            cur = None
            if hist_len >= 2:
                try:
                    prev = hist[-2]
                except Exception:
                    prev = None
                try:
                    cur = hist[-1]
                except Exception:
                    cur = None

            core = _import_scanner_core()
            res = core.detect_flip_current_open(hist, hist_threshold)

            try:
                logger.debug(
                    "[DETECT_FLIP] %s %s hist_len=%d prev=%s cur=%s last3=%s threshold=%s -> flip=%s",
                    symbol or "-", tf or "-", hist_len,
                    ("{:.12g}".format(prev) if prev is not None else "None"),
                    ("{:.12g}".format(cur) if cur is not None else "None"),
                    last3, hist_threshold, res
                )
            except Exception:
                logger.debug("[DETECT_FLIP] %s %s hist_len=%d prev=%s cur=%s last3=%s threshold=%s -> flip=%s",
                             symbol or "-", tf or "-", hist_len, str(prev), str(cur), str(last3), str(hist_threshold), str(res))
            return res
        except Exception as e:
            logger.exception("Diagnostic detect_flip_current_open failed: %s", e)
            try:
                core = _import_scanner_core()
                return core.detect_flip_current_open(hist, hist_threshold)
            except Exception:
                return False

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        try:
            names = ["get_24h_ticker", "get24h", "get_24h", "get_ticker_24h", "ticker_24h", "get_ticker"]
            data = await self._call_client_method(names, symbol)
            if not data:
                logger.debug("[VOLUME_UPDATE] No ticker data returned for %s", symbol)
                return None

            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], (dict, list)):
                    if isinstance(data["data"], dict):
                        data = data["data"]
                elif "result" in data and isinstance(data["result"], (dict, list)):
                    if isinstance(data["result"], dict):
                        data = data["result"]

            vol = None
            if isinstance(data, dict):
                for key in ("volume", "vol", "turnover", "volume24h", "quote_vol", "volume_24h"):
                    if key in data and data.get(key) is not None:
                        try:
                            vol = float(data.get(key))
                            break
                        except Exception:
                            try:
                                vol = float(str(data.get(key)).replace(',', ''))
                                break
                            except Exception:
                                vol = None

            if vol is None and isinstance(data, (int, float, str)):
                try:
                    vol = float(data)
                except Exception:
                    vol = None

            if vol is not None:
                if symbol not in self._24h_volumes:
                    self._24h_volumes[symbol] = {"current": vol, "previous": vol}
                else:
                    self._24h_volumes[symbol]["previous"] = self._24h_volumes[symbol]["current"]
                    self._24h_volumes[symbol]["current"] = vol
                logger.debug("[VOLUME_UPDATE] %s: current=%.0f", symbol, vol)
                prev = self._24h_volumes[symbol].get("previous", 0)
                curr = self._24h_volumes[symbol].get("current", 0)
                logger.debug("[VOL_DEBUG] prev=%s, curr=%s", prev, curr)
                return vol
            else:
                logger.debug("[VOLUME_UPDATE] %s: ticker returned but no volume field matched: %s", symbol, data if isinstance(data, dict) else str(data))
        except Exception:
            logger.debug("Could not update 24h volume for %s", symbol, exc_info=True)
        return None

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        core = _import_scanner_core()
        return core.compute_24h_volume_change_from(self._24h_volumes.get(symbol))

    def compute_tv_rating(self, symbol: str, tf: str, price: Optional[float] = None):
        klines = self.kline_store.get(symbol, {}).get(tf, [])
        core = _import_scanner_core()
        return core.compute_tv_rating_from(klines, TECHNICAL_RATING, tf=tf, price=price)

    def _compute_mtf_alignment(self, symbol: str, price: float):
        core = _import_scanner_core()

        def _get_closes(tf: str) -> List[float]:
            items = self.kline_store.get(symbol, {}).get(tf, [])
            closes = []
            for c in items:
                try:
                    if isinstance(c, dict) and c.get("close") is not None:
                        closes.append(float(c.get("close")))
                    elif isinstance(c, (int, float)):
                        closes.append(float(c))
                except Exception:
                    continue
            return closes
        return core.compute_mtf_alignment(_get_closes, price, MTF_ALIGN_TFS, mtf_slope_lookback=MTF_SLOPE_LOOKBACK)

    def _is_candle_age_acceptable(self, start_at: Optional[int], now: float) -> bool:
        core = _import_scanner_core()
        return core.is_candle_age_acceptable(start_at, now, FLIP_CANDLE_AGE_MAX_SEC)

    def _try_dedupe_signal(self, symbol: str, tf: str, candle_open_time: Optional[int], now: float) -> bool:
        if SIGNAL_DEDUP_WINDOW <= 0:
            return True

        if candle_open_time is None:
            logger.debug("Cannot dedupe signal: candle_open_time is None")
            return True

        try:
            cache_key = (symbol, tf, int(candle_open_time))
            if cache_key in self._signal_cache:
                last_signal_time = self._signal_cache[cache_key]
                time_since_last = now - last_signal_time
                if time_since_last < SIGNAL_DEDUP_WINDOW:
                    logger.debug(
                        "DEDUPE BLOCKED: %s %s candle_open=%d (last signal %.0f sec ago, window=%.0f sec)",
                        symbol, tf, candle_open_time, time_since_last, SIGNAL_DEDUP_WINDOW
                    )
                    return False
            self._signal_cache[cache_key] = now
            logger.debug("DEDUPE PASSED: %s %s candle_open=%d (new or expired)", symbol, tf, candle_open_time)
            return True
        except Exception as e:
            logger.debug("Error in dedupe check: %s", e)
            return True

    async def _wait_until_next_scan_boundary(self) -> float:
        now = time.time()
        if ROOT_SCAN_INTERVAL and ROOT_SCAN_INTERVAL > 0:
            next_boundary = (int(now / ROOT_SCAN_INTERVAL) + 1) * ROOT_SCAN_INTERVAL
            to_sleep = next_boundary - now
            logger.info(
                "[SCAN_BOUNDARY] ROOT_SCAN_INTERVAL=%.0f: sleeping %.3f sec to align to boundary (ts=%d)",
                ROOT_SCAN_INTERVAL, to_sleep, int(next_boundary)
            )
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)
            return float(next_boundary)
        else:
            FIVE_MIN = 300
            next_boundary = ((int(now) // FIVE_MIN) + 1) * FIVE_MIN
            to_sleep = max(0.0, next_boundary - now)
            next_struct = time.gmtime(next_boundary)
            logger.info(
                "[SCAN_BOUNDARY] ROOT_SCAN_INTERVAL=0: aligning to next 5m boundary at %02d:%02d:%02d UTC (sleeping %.3f sec)",
                next_struct.tm_hour, next_struct.tm_min, next_struct.tm_sec, to_sleep
            )
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)
            return float(next_boundary)

    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s ROOT_TFS=%s", ROOT_SCAN_INTERVAL, ROOT_TFS)
        loop_count = 0

        while not self._stop:
            loop_count += 1
            logger.info("=" * 80)
            logger.info("[LOOP_START] Cycle #%d", loop_count)

            logger.info("[BOUNDARY_WAIT] Waiting for next scan boundary (interval=%s)...", ROOT_SCAN_INTERVAL)
            try:
                boundary_ts = await self._wait_until_next_scan_boundary()
                logger.info("[BOUNDARY_REACHED] Woke at ts=%d", int(boundary_ts))
            except Exception as e:
                logger.exception("[BOUNDARY_ERROR] %s", e)
                await asyncio.sleep(5)
                continue

            start_cycle = time.time()
            try:
                logger.info("[PHASE_1] Checking if symbols need discovery (current=%d)", len(self.symbols))
                if not self.symbols:
                    logger.info("[DISCOVER] No symbols cached, discovering...")
                    try:
                        await self.discover_symbols()
                        logger.info("[DISCOVER_OK] Found %d symbols", len(self.symbols))
                    except Exception as e:
                        logger.exception("[DISCOVER_FAIL] %s", e)
                        await asyncio.sleep(10)
                        continue

                    if not self.symbols:
                        logger.warning("[DISCOVER_EMPTY] Symbol discovery returned 0 symbols")
                        await asyncio.sleep(10)
                        continue

                logger.info("[PHASE_2] Checking if kline refresh needed...")
                now = float(boundary_ts)
                need_seed = False
                refreshed_tfs = []

                for root in ROOT_TFS:
                    current_start = self._get_current_candle_start(root, now)
                    last_start = self._last_tf_candle_open_times.get(root, 0.0)
                    if current_start > last_start:
                        logger.info("[CANDLE_OPEN] New candle for %s: current=%d > last=%d", root, current_start, int(last_start))
                        self._last_tf_candle_open_times[root] = float(current_start)
                        refreshed_tfs.append(root)
                        need_seed = True

                if any(self._last_tf_candle_open_times.get(tf, 0.0) == 0.0 for tf in ROOT_TFS):
                    logger.info("[FIRST_CYCLE] First scan cycle - need to seed all klines")
                    need_seed = True
                    refreshed_tfs = ROOT_TFS if not refreshed_tfs else refreshed_tfs

                # If a new candle opened, do a fast refresh of the last candle for immediate scanning,
                # and schedule a full seed in background (non-blocking) so scanning isn't delayed.
                if need_seed:
                    try:
                        logger.info("[REFRESH_FAST] New candle detected; performing fast refresh for immediate scan")
                        # Fast refresh latest candle only (limit=2)
                        await self._fast_refresh_latest(self.symbols, refreshed_tfs, limit=2)
                        # schedule full seeding in background if not already running
                        if not self._seeding_task or self._seeding_task.done():
                            logger.info("[SEED_BG] Scheduling full seed in background")
                            self._seeding_task = asyncio.create_task(self.seed_all())
                    except Exception as e:
                        logger.exception("[REFRESH_FAIL] %s", e)
                else:
                    logger.debug("[SEED_SKIP] No new candles, using cached klines")

                await self._ensure_rest_poller()

                logger.info("[PHASE_3_START] ===================== SIGNAL SCAN PHASE =====================")
                root_signals: List[Dict[str, Any]] = []
                signal_count = 0

                async def check_symbol(sym: str):
                    nonlocal signal_count
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
                                pass

                        if price is None:
                            logger.debug("[SKIP_PRICE] %s - no price available", sym)
                            return

                        self._last_price_cache[sym] = price
                        logger.debug("[PRICE_OK] %s: %.6f", sym, price)

                        await self._update_24h_volume(sym)
                        now_check = time.time()

                        for root in ROOT_TFS:
                            logger.debug("[CHECK_TF_START] %s %s", sym, root)

                            macd_line, sig, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)

                            if not hist or len(hist) < 2:
                                logger.debug("[HIST_TOO_SHORT] %s %s: len=%d", sym, root, len(hist) if hist else 0)
                                continue

                            prev_val = hist[-2]
                            cur_val = hist[-1]
                            zero_cross = prev_val <= 0 and cur_val > 0

                            logger.info("[HIST_VAL] %s %s: prev=%.8f cur=%.8f ZERO_CROSS=%s", sym, root, prev_val, cur_val, zero_cross)

                            # Use diagnostic detect wrapper
                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)
                            logger.info("[FLIP_CHECK] %s %s: flip=%s", sym, root, flip)

                            if not flip:
                                logger.debug("[NO_FLIP] %s %s - no flip detected", sym, root)
                                continue

                            signal_count += 1
                            logger.info("[***SIGNAL***] #%d: %s %s @ price=%.6f", signal_count, sym, root, price)

                            vol_info = self._24h_volumes.get(sym, {})
                            vol_change = vol_info.get("volume_change")
                            vol_usdt = vol_info.get("volume_usdt")

                            start_at = None
                            try:
                                last_candles = self.kline_store.get(sym, {}).get(root, [])
                                if last_candles:
                                    start_at = last_candles[-1].get("start_at")
                            except Exception:
                                pass

                            candle_age_ok = self._is_candle_age_acceptable(start_at, now_check)
                            is_new_signal = self._try_dedupe_signal(sym, root, start_at, now_check)

                            if not is_new_signal:
                                logger.info("[DEDUPE_BLOCK] %s %s - duplicate", sym, root)
                                continue

                            tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                            mtf_align = self._compute_mtf_alignment(sym, price)

                            sig_item = {
                                "symbol": sym,
                                "root": root,
                                "price": price,
                                "hist": hist,
                                "vol_change": vol_change,
                                "volume_usdt": vol_usdt,
                                "start_at": start_at,
                                "tv_score": tv_score,
                                "tv_label": tv_label,
                                "candle_age_ok": candle_age_ok,
                                "mtf_status": mtf_align.get("status", "N/A"),
                                "negative_tfs": mtf_align.get("negative_tfs", []),
                                "score": sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip")) + (min(vol_change, 1.0) if vol_change is not None and vol_change > 0 else 0.0)
                            }
                            root_signals.append(sig_item)
                            logger.info("[SIGNAL_QUEUED] %s %s tv=%.4f mtf=%s", sym, root, tv_score, mtf_align.get("status"))

                    except Exception as e:
                        logger.exception("[SYMBOL_ERROR] %s: %s", sym, e)

                logger.info("[SCAN_START] Scanning %d symbols", len(self.symbols))
                checked = 0
                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        logger.info("[SCAN_STOPPED] _stop flag set")
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    logger.info("[BATCH] Processing symbols %d-%d", i, i + len(batch) - 1)
                    tasks = [asyncio.create_task(check_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks)
                    checked += len(batch)
                    if i + REQUEST_BATCH_SIZE < len(self.symbols):
                        await asyncio.sleep(REQUEST_BATCH_DELAY)

                logger.info("[PHASE_3_DONE] Checked %d symbols, found %d SIGNALS", checked, len(root_signals))

                # PHASE 4 and beyond unchanged (handle signals, telegram, opens)
                newly_aligned = await self._check_monitored_symbols()
                evaluated_aligned = []
                if newly_aligned:
                    evaluated_aligned = await self.handle_root_signals(newly_aligned, allow_open_trades=True)

                now_ts = float(boundary_ts)
                is_full_push = self.telegram.check_full_push(now_ts)

                if root_signals and is_full_push:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("MTF subscribe failed for %s", sig.get("symbol"))

                evaluated_signals = []
                if root_signals:
                    logger.info("[EVAL] Evaluating %d signals", len(root_signals))
                    evaluated_signals = await self.handle_root_signals(root_signals, allow_open_trades=is_full_push)

                if newly_aligned:
                    root_signals.extend(newly_aligned)
                    evaluated_signals.extend(evaluated_aligned)

                if hasattr(self.telegram, "send_summary"):
                    try:
                        await self.telegram.send_summary(
                            root_signals=root_signals,
                            evaluated=evaluated_signals,
                            full_push=is_full_push,
                            is_candle_open=is_full_push
                        )
                        logger.info("[TELEGRAM_SENT]")
                    except Exception:
                        logger.exception("Telegram send failed")

                if is_full_push:
                    self.telegram.mark_full_push_sent()

                elapsed = time.time() - start_cycle
                logger.info("[CYCLE_END] Cycle #%d complete in %.2f sec (signals=%d)", loop_count, elapsed, len(root_signals))

            except Exception as e:
                logger.exception("[CYCLE_EXCEPTION] Cycle #%d: %s", loop_count, e)

            logger.info("=" * 80)

    async def _check_monitored_symbols(self) -> List[Dict[str, Any]]:
        newly_aligned: List[Dict[str, Any]] = []
        if not self._mtf_monitoring:
            return newly_aligned

        MONITORING_MAX_AGE = 86400
        now = time.time()
        to_remove: List[str] = []

        for sym, info in list(self._mtf_monitoring.items()):
            try:
                if now - info.get("started_at", now) > MONITORING_MAX_AGE:
                    logger.info("MONITORING EXPIRED (24h): %s – removing", sym)
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
                    logger.info("MONITORING RESOLVED: %s → %s – queuing trade open", sym, status)
                    to_remove.append(sym)
                    vol_change = self.compute_24h_volume_change(sym)
                    resolved_item = {
                        "symbol": sym,
                        "root": info["root"],
                        "price": price,
                        "hist": [],
                        "vol_change": vol_change,
                        "from_monitoring": True,
                        "mtf_status": status,
                        "negative_tfs": mtf_align.get("negative_tfs", []),
                        "score": sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip")) + (min(vol_change, 1.0) if vol_change is not None and vol_change > 0 else 0.0)
                    }
                    newly_aligned.append(resolved_item)
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

        return newly_aligned

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]], allow_open_trades: bool = True) -> List[Dict[str, Any]]:
        evaluated: List[Dict[str, Any]] = []
        to_open: List[Dict[str, Any]] = []

        for item in root_signals:
            sym = item["symbol"]
            price = item["price"]
            root = item["root"]
            vol_change = item.get("vol_change")
            tv_label = item.get("tv_label")
            tv_score = item.get("tv_score", 0.0)
            start_at = item.get("start_at")

            candle_age_ok = item.get("candle_age_ok")
            if candle_age_ok is None:
                candle_age_ok = self._is_candle_age_acceptable(start_at, time.time())

            hist = item.get("hist", [])
            if not hist:
                _, _, hist = self.compute_macd_for(sym, root, include_price=price)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            mtf_align = self._compute_mtf_alignment(sym, price)
            mtf_status = mtf_align["status"]
            negative_tfs = mtf_align.get("negative_tfs", [])

            score = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive"))
            score += sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))
            if vol_change is not None and vol_change > 0:
                score += min(vol_change, 1.0)

            entry: Dict[str, Any] = {
                "symbol": sym,
                "root": root,
                "price": price,
                "hist": hist,
                "macd_hist_val": macd_hist_val,
                "mtf": mtf_align["tfs"],
                "mtf_status": mtf_status,
                "negative_tfs": negative_tfs,
                "one_d_slope": mtf_align.get("one_d_slope"),
                "vol_change": vol_change,
                "score": score,
                "accept": False,
                "reason": "pending",
                "tv_label": tv_label,
                "tv_score": tv_score,
            }

            if mtf_status in ("aligned", "daily_rising"):
                if not candle_age_ok:
                    entry["accept"] = False
                    entry["reason"] = "candle_too_old"
                    logger.info("Trade blocked by FLIP_CANDLE_AGE_MAX_SEC (candle too old): %s root=%s", sym, root)
                    evaluated.append(entry)
                    continue

                if TRADE_RATING_MIN_VAL > 0.0 and tv_score < TRADE_RATING_MIN_VAL:
                    entry["accept"] = False
                    entry["reason"] = f"tv_rating_below_threshold_{tv_score:.4f}"
                    logger.info("Trade blocked by TRADE_RATING_MIN: %s tv_score=%.4f < min=%.4f", sym, tv_score, TRADE_RATING_MIN_VAL)
                    evaluated.append(entry)
                    continue

                if MARKET_CAP_MIN and MARKET_CAP_MIN > 0:
                    try:
                        symbol_info = await self.client.get_symbol_info(sym)
                        marketcap = None
                        if isinstance(symbol_info, dict):
                            for key in ("market_cap", "marketCap", "market_cap_usd", "marketcap"):
                                if key in symbol_info and symbol_info.get(key) is not None:
                                    try:
                                        marketcap = float(symbol_info.get(key))
                                        break
                                    except Exception:
                                        try:
                                            marketcap = float(str(symbol_info.get(key)).replace(',', ''))
                                            break
                                        except Exception:
                                            marketcap = None
                        if marketcap is not None and marketcap < MARKET_CAP_MIN:
                            entry["accept"] = False
                            entry["reason"] = "market_cap_filtered"
                            logger.info("Market cap filter blocked %s: cap=%s < min=%s", sym, marketcap, MARKET_CAP_MIN)
                            evaluated.append(entry)
                            continue
                    except Exception:
                        logger.exception("Market cap check failed for %s", sym)

                if VOLUME_FILTER_ENABLED:
                    if vol_change is None or vol_change < VOLUME_MIN_CHANGE_PCT:
                        entry["accept"] = False
                        entry["reason"] = "vol_filter_blocked"
                        evaluated.append(entry)
                        continue
                    else:
                        entry["accept"] = True
                        entry["reason"] = mtf_status
                        to_open.append(entry)
                else:
                    if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                        entry["accept"] = False
                        entry["reason"] = "negvol_blocked"
                        evaluated.append(entry)
                        continue
                    else:
                        entry["accept"] = True
                        entry["reason"] = mtf_status
                        to_open.append(entry)

            elif mtf_status == "monitoring":
                entry["reason"] = "monitoring"
                if sym not in self._mtf_monitoring:
                    self._mtf_monitoring[sym] = {
                        "root": root,
                        "price": price,
                        "started_at": time.time(),
                        "negative_tfs": list(negative_tfs),
                        "last_alert": 0.0,
                    }

            evaluated.append(entry)

        await self._emit_event("candidates_evaluated", evaluated)

        candidates = to_open

        if TRADE_RATING_PRIORITIZE and candidates:
            candidates = sorted(candidates, key=lambda c: c.get("tv_score", 0.0), reverse=True)
            logger.info("Sorted %d candidates by TV rating (highest first)", len(candidates))

        if ROOT_FILTER:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for c in candidates:
                grouped.setdefault(c["root"], []).append(c)

            selected: List[Dict[str, Any]] = []
            order_list = PRIORITIZE_SLOT_ORDER if PRIORITIZE_SLOT_ORDER else ROOT_TFS
            remaining_slots = max(0, MAX_OPEN_TRADES - (len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0))

            for rt in order_list:
                if remaining_slots <= 0:
                    break
                lst = grouped.get(rt, [])
                if not lst:
                    continue
                top = lst[:remaining_slots]
                selected.extend(top)
                remaining_slots -= len(top)

            if remaining_slots > 0:
                remaining_candidates = [c for c in candidates if c not in selected]
                remaining_sorted = remaining_candidates[:remaining_slots]
                selected.extend(remaining_sorted)

            candidates = sorted(selected, key=lambda r: self._compute_combined_score(r), reverse=True)
        else:
            if PRIORITIZE_SLOT_ORDER:
                remaining_slots = max(0, MAX_OPEN_TRADES - (len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0))
                selected: List[Dict[str, Any]] = []
                grouped: Dict[str, List[Dict[str, Any]]] = {}
                for c in candidates:
                    grouped.setdefault(c["root"], []).append(c)
                for rt in PRIORITIZE_SLOT_ORDER:
                    if remaining_slots <= 0:
                        break
                    lst = grouped.get(rt, [])
                    if not lst:
                        continue
                    top = lst[:remaining_slots]
                    selected.extend(top)
                    remaining_slots -= len(top)
                if remaining_slots > 0:
                    remaining_candidates = [c for c in candidates if c not in selected]
                    remaining_sorted = remaining_candidates[:remaining_slots]
                    selected.extend(remaining_sorted)
                candidates = sorted(selected, key=lambda r: self._compute_combined_score(r), reverse=True)
            else:
                candidates = sorted(candidates, key=lambda r: self._compute_combined_score(r), reverse=True)

        eval_map: Dict[tuple, Dict[str, Any]] = {(e["symbol"], e["root"]): e for e in evaluated}

        if not allow_open_trades:
            for c in candidates:
                c["open_suppressed"] = True
                eval = eval_map.get((c["symbol"], c["root"]))
                if eval is not None:
                    eval["open_suppressed"] = True
                    eval["accept"] = False
                    eval["reason"] = "open_suppressed"
            return evaluated

        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Max open trades reached – halting further opens.")
                break

            sym = c["symbol"]
            price = c["price"]
            vol_change = c.get("vol_change")

            if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                c["accept"] = False
                c["reason"] = "negvol_blocked"
                eval = eval_map.get((c["symbol"], c["root"]))
                if eval is not None:
                    eval["accept"] = False
                    eval["reason"] = "negvol_blocked"
                continue

            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None

            try:
                symbol_info = await self.client.get_symbol_info(sym)
            except Exception:
                symbol_info = {}

            qty_raw = self.trade_manager.compute_qty_from_balance(balance if balance else 0.0, price, symbol_info)
            qty = self._quantize_qty(qty_raw, symbol_info.get("step") if symbol_info else None, symbol_info.get("min_qty") if symbol_info else None)
            if qty <= 0 or math.isclose(qty, 0.0):
                c["accept"] = False
                c["reason"] = "zero_qty"
                eval = eval_map.get((c["symbol"], c["root"]))
                if eval is not None:
                    eval["accept"] = False
                    eval["reason"] = "zero_qty"
                continue

            side = "Buy"
            reason_tag = c.get("reason", "signal")
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    await self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    eval = eval_map.get((sym, c["root"]))
                    if eval is not None:
                        eval["accept"] = True
                        eval["reason"] = "opened"
                        eval["order"] = order
                    await send_message(
                        f"✅ Trade Opened – {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                    )
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
                    c["accept"] = False
                    c["reason"] = "order_failed"
                    eval = eval_map.get((sym, c["root"]))
                    if eval is not None:
                        eval["accept"] = False
                        eval["reason"] = "order_failed"
            else:
                await self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"], "tv_score": c.get("tv_score", 0.0)})
                eval = eval_map.get((sym, c["root"]))
                if eval is not None:
                    eval["accept"] = True
                    eval["reason"] = "simulated"
                    eval["simulated"] = True
                await send_message(
                    f"🔔 Simulated Trade – {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                )

            self._mtf_monitoring.pop(sym, None)

        return evaluated

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - TV_RATING_WEIGHT)) + (tv_score * TV_RATING_WEIGHT)
        return combined

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

# --- DIAGNOSTIC: optional background start for quick testing ----
if os.getenv("DIAGNOSTIC_START_SCANNER", "").strip().lower() in ("1", "true", "yes", "y"):
    import threading

    def _diagnostic_start():
        try:
            logger.info("[DIAG_START] Diagnostic scanner background starter launching")
            s = Scanner()
            asyncio.run(s.run())
        except Exception:
            logger.exception("[DIAG_START] Diagnostic scanner failed")

    t = threading.Thread(target=_diagnostic_start, name="diag-scanner", daemon=True)
    t.start()
    logger.info("[DIAG_START] Diagnostic scanner thread created (look for '[DIAGNOSTIC] root_scan_loop: STARTING')")
