# scanner_scan.py
# Scanning, kline storage, seeding, REST fallback, MACD computation, signal detection, dedupe/tracking.
import os
import asyncio
import time
import json
import math
import inspect
from typing import Dict, List, Any, Optional, Callable, Tuple, Iterable
from collections import defaultdict

from .logger import get_logger
from .bybit_client import BybitClient
from .ratelimiter import TokenBucket
from .scanner_core import (
    tf_to_seconds,
    normalize_klines,
    compute_macd_from_closes,
    detect_flip_current_open,
    compute_24h_volume_change_from,
    compute_tv_rating_from,
    compute_mtf_alignment,
)
from .scanner_telegram import TelegramSummary
from .telegram import send_message
from . import config as cfg

logger = get_logger("scanner.scan")

# Config with safe defaults
EXCLUDE_STABLECOINS = getattr(cfg, "EXCLUDE_STABLECOINS", [])
CONCURRENCY = int(getattr(cfg, "CONCURRENCY", 4))
KLINE_SEED_LIMIT = int(getattr(cfg, "KLINE_SEED_LIMIT", 100))
ROOT_TFS = getattr(cfg, "ROOT_TFS", ["5", "15", "60", "240"])
MTF_TFS = getattr(cfg, "MTF_TFS", ["5", "15", "60", "240", "D"])
ROOT_SCAN_INTERVAL = float(getattr(cfg, "ROOT_SCAN_INTERVAL", 0) or 0)
TRADE_ENABLED = getattr(cfg, "TRADE_ENABLED", False)
MTF_SLOPE_LOOKBACK = int(getattr(cfg, "MTF_SLOPE_LOOKBACK", 3))
ROOT_FILTER = getattr(cfg, "ROOT_FILTER", False)
ROOT_TOP_N = getattr(cfg, "ROOT_TOP_N", None)
MAX_OPEN_TRADES = int(getattr(cfg, "MAX_OPEN_TRADES", 5))
USE_WS = getattr(cfg, "USE_WS", False)
MAX_CONCURRENT_REQUESTS = int(getattr(cfg, "MAX_CONCURRENT_REQUESTS", 8))
REQUEST_BATCH_SIZE = int(getattr(cfg, "REQUEST_BATCH_SIZE", 32))
REQUEST_BATCH_DELAY = float(getattr(cfg, "REQUEST_BATCH_DELAY", 0.1))
REST_POLL_INTERVAL = float(getattr(cfg, "REST_POLL_INTERVAL", 10))
VOLUME_FILTER_ENABLED = getattr(cfg, "VOLUME_FILTER_ENABLED", False)
VOLUME_MIN_CHANGE_PCT = float(getattr(cfg, "VOLUME_MIN_CHANGE_PCT", 0.15))
TECHNICAL_RATING = getattr(cfg, "TECHNICAL_RATING", {})
FLIP_CANDLE_AGE_MAX_SEC = int(getattr(cfg, "FLIP_CANDLE_AGE_MAX_SEC", 120))
SIGNAL_DEDUP_WINDOW = int(getattr(cfg, "SIGNAL_DEDUP_WINDOW", 30))
TRADE_RATING_MIN = float(getattr(cfg, "TRADE_RATING_MIN", 0))
TRADE_RATING_PRIORITIZE = getattr(cfg, "TRADE_RATING_PRIORITIZE", False)
TRADE_NO_NEG_VOL = os.getenv("TRADE_NO_NEG_VOL", "1").strip().lower() in ("1", "true", "yes", "y")
try:
    MARKET_CAP_MIN = float(os.getenv("MARKET_CAP_MIN", str(getattr(cfg, "MARKET_CAP_MIN", 0)) or 0))
except Exception:
    MARKET_CAP_MIN = 0.0
try:
    TV_RATING_WEIGHT = float(os.getenv("TV_RATING_WEIGHT", "0.3"))
    TV_RATING_WEIGHT = max(0.0, min(1.0, TV_RATING_WEIGHT))
except Exception:
    TV_RATING_WEIGHT = getattr(cfg, "TV_RATING_WEIGHT", 0.3)
PRIORITIZE_SLOT_ORDER = [p.strip() for p in (getattr(cfg, "PRIORITIZE_SLOT_ORDER", os.getenv("PRIORITIZE_SLOT_ORDER", "240,D,60").split(","))) if p.strip()]

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
TELEGRAM_DISPATCH_WINDOW = 5

# Minimum candles recommended for MACD histogram stability
MIN_MACD_CANDLES = 26


class ScannerScan:
    def __init__(self, client: BybitClient, rate_limiter: TokenBucket):
        self.rate_limiter = rate_limiter
        self.client = client
        self.trade_manager = None
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
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        self._symbol_check_count = 0

        self._last_tf_candle_open_times: Dict[str, float] = {tf: 0.0 for tf in ROOT_TFS}
        self._last_telegram_dispatch_time: Optional[float] = None

        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        self.telegram = TelegramSummary()

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) "
            "TRADE_RATING_MIN=%s TV_RATING_WEIGHT=%.2f TRADE_RATING_PRIORITIZE=%s FLIP_CANDLE_AGE_MAX_SEC=%d SIGNAL_DEDUP_WINDOW=%d "
            "TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s ROOT_SCAN_INTERVAL=%s",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN, TV_RATING_WEIGHT, TRADE_RATING_PRIORITIZE, FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW,
            TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER, ROOT_SCAN_INTERVAL
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

    async def _call_client_method(self, names: Iterable[str], *args, **kwargs):
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
        logger.debug("No client method among %s succeeded", list(names))
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
        if SEED_KLINES_LIMIT < MIN_MACD_CANDLES:
            logger.warning("SEED_KLINES_LIMIT is very low (%d); MACD requires >=%d for stability", SEED_KLINES_LIMIT, MIN_MACD_CANDLES)

        tfs = list(set(ROOT_TFS + MTF_TFS + MTF_ALIGN_TFS))
        for tf in tfs:
            try:
                limit = SEED_KLINES_LIMIT
                logger.debug("seed_klines_for_symbol: requesting %s %s with limit=%d", symbol, tf, limit)
                async with self.request_sem:
                    raw = await self._call_get_klines(symbol, tf, limit=limit)

                if not raw:
                    logger.debug("No klines returned for %s %s (raw empty)", symbol, tf)
                    continue

                normalized = normalize_klines(raw, tf)

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
                                normalized = normalize_klines(data, tf) if data else []

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

    # Helpers wrappers
    def _tf_to_seconds(self, tf: str) -> int:
        return tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str):
        return normalize_klines(raw_klines, tf)

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        from .scanner_core import quantize_qty  # local import
        return quantize_qty(qty, step, min_qty)

    async def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        """
        Async compute MACD histogram for a symbol/tf.
        - Uses kline_store closes when available
        - If closes < MIN_MACD_CANDLES, attempts a fallback REST fetch to expand history (await)
        - include_price overwrites the last close
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

        # get current price candidate from ws if requested
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

        # If insufficient stored closes, attempt to fetch a fallback history (best-effort)
        if len(closes) < MIN_MACD_CANDLES:
            fallback_limit = max(SEED_KLINES_LIMIT, MIN_MACD_CANDLES)
            try:
                logger.debug("INSUFFICIENT CLOSES for %s %s: have=%d, attempting fallback request with limit=%d", symbol, tf, len(closes), fallback_limit)
                async with self.request_sem:
                    raw = await self._call_get_klines(symbol, tf, limit=fallback_limit)
                if raw:
                    normalized = normalize_klines(raw, tf)
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
                    if valid:
                        try:
                            klines_sorted = sorted(valid, key=lambda x: x.get("start_at") or 0)
                        except Exception:
                            klines_sorted = valid
                        # Update kline_store so subsequent evaluations benefit
                        self.kline_store.setdefault(symbol, {})[tf] = klines_sorted
                        closes = [float(x["close"]) for x in klines_sorted if x.get("close") is not None]
                        logger.debug("Fallback seed updated kline_store for %s %s: new_count=%d", symbol, tf, len(closes))
            except Exception:
                logger.exception("Fallback kline fetch failed for %s %s", symbol, tf)

        # Ensure we have at least one data point: if none but we have current price, use it
        if not closes and current_price is not None:
            closes = [current_price]

        # Include current price (overwrite last close) when provided
        try:
            macd_line, signal_line, hist = compute_macd_from_closes(closes, include_price=current_price)
        except Exception:
            try:
                macd_line, signal_line, hist = compute_macd_from_closes([current_price] if current_price is not None else [], include_price=None)
            except Exception:
                macd_line, signal_line, hist = ([], [], [])

        return macd_line, signal_line, hist

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        return detect_flip_current_open(hist, hist_threshold)

    # (rest of methods remain same, including _compute_mtf_alignment, _compute_current_candle_start, dedupe, root_scan_loop)
    # For brevity the root_scan_loop below is the same as previous version but now calls:
    # macd_line, sig, hist = await self.compute_macd_for(...)
    # and uses _detect_tf_candle_opens(...) to force full_push.
    #
    # Full root_scan_loop (unchanged logic except awaiting compute_macd_for):
    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1
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
                                if ROOT_TFS and USE_WS and self.client.is_ws_connected():
                                    ws_last = self.client.get_ws_latest_kline(sym, ROOT_TFS[0]) if hasattr(self.client, "get_ws_latest_kline") else None
                                    if ws_last and ws_last.get("close") is not None:
                                        price = float(ws_last.get("close"))
                            except Exception:
                                price = None

                        if price is None:
                            return

                        self._last_price_cache[sym] = price
                        await self._update_24h_volume(sym)
                        now = time.time()

                        for root in ROOT_TFS:
                            logger.info("[ROOT_SCAN_CALC] %s %s: STARTING MACD calculation", sym, root)
                            macd_line, sig, hist = await self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)

                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)
                            logger.info("[ROOT_SCAN_RESULT] %s %s: flip_detected=%s hist_len=%d", sym, root, flip, len(hist) if hist else 0)

                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                start_at = None
                                try:
                                    start_at = self._compute_current_candle_start(root, now=now)
                                except Exception:
                                    start_at = None

                                candle_age_ok = self._is_candle_age_acceptable(start_at, now)
                                is_new_signal = self._try_dedupe_signal(sym, root, start_at, now)

                                if not candle_age_ok:
                                    logger.info("SIGNAL REJECTED (candle too old): %s %s @ %s", sym, root, price)
                                    continue

                                if not is_new_signal:
                                    logger.info("SIGNAL REJECTED (duplicate): %s %s @ %s", sym, root, price)
                                    continue

                                tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                                mtf_align = self._compute_mtf_alignment(sym, price)

                                sig_item = {
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change,
                                    "start_at": start_at,
                                    "tv_score": tv_score,
                                    "tv_label": tv_label,
                                    "mtf_status": mtf_align.get("status", "N/A"),
                                    "negative_tfs": mtf_align.get("negative_tfs", []),
                                    "score": sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip")) + (min(vol_change, 1.0) if vol_change is not None and vol_change > 0 else 0.0)
                                }
                                root_signals.append(sig_item)
                                logger.info("SIGNAL DETECTED: %s %s @ %s (tv=%s %+.3f candle_age=%.0f sec start_at=%s)",
                                            sym, root, price, tv_label, tv_score, now - (start_at if start_at else now), start_at)

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

                logger.info("[DIAGNOSTIC] root_scan_loop: Checked %d symbols, found %d signals", checked_count, len(root_signals))

                newly_aligned = await self._check_monitored_symbols()
                if newly_aligned:
                    await self._emit_event("root_signals_ready", {"root_signals": newly_aligned, "allow_open_trades": True})

                now_ts = time.time()
                opened_tfs = self._detect_tf_candle_opens(now_ts)
                if opened_tfs:
                    logger.info("[CANDLE_OPEN] Detected new candle opens for root TFs: %s", opened_tfs)
                    is_full_push = True
                else:
                    is_full_push = self.telegram.check_full_push(now_ts)

                if root_signals and is_full_push:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))

                if root_signals:
                    await self._emit_event("root_signals_ready", {"root_signals": root_signals, "allow_open_trades": is_full_push})

                evaluated_signals = []
                if hasattr(self.telegram, "send_summary"):
                    try:
                        await self.telegram.send_summary(
                            root_signals=root_signals + newly_aligned,
                            evaluated=evaluated_signals,
                            full_push=is_full_push,
                            is_candle_open=bool(opened_tfs)
                        )
                    except Exception:
                        logger.exception("Failed to dispatch Telegram summary")

                if is_full_push:
                    self.telegram.mark_full_push_sent()

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start

            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle (ROOT_SCAN_INTERVAL=%.1f)", to_sleep, ROOT_SCAN_INTERVAL)
                await asyncio.sleep(to_sleep)
            else:
                now = time.time()
                now_struct = time.gmtime(now)
                current_minute = now_struct.tm_min
                current_second = now_struct.tm_sec

                next_5m_minute = ((current_minute // 5) + 1) * 5

                if next_5m_minute >= 60:
                    to_sleep = (60 - current_minute) * 60 - current_second
                else:
                    to_sleep = ((next_5m_minute - current_minute) * 60) - current_second

                to_sleep = max(1, min(300, to_sleep))
                logger.info("[DIAGNOSTIC] Aligning to next 5m candle open: sleeping %.1f seconds (current=%02d:%02d, target=:%02d:00)",
                           to_sleep, current_minute, current_second, next_5m_minute % 60)
                await asyncio.sleep(to_sleep)

    async def handle_root_signals_local(self, root_signals: List[Dict[str, Any]], allow_open_trades: bool = True):
        await self._emit_event("root_signals_ready", {"root_signals": root_signals, "allow_open_trades": allow_open_trades})

    def stop(self):
        logger.info("Stopping scanner scan module...")
        self._stop = True
        if self._task and not self._task.done():
            try:
                self._task.cancel()
            except Exception:
                pass
        if self._rest_poller_task and not self._rest_poller_task.done():
            try:
                self._rest_poller_task.cancel()
            except Exception:
                pass
