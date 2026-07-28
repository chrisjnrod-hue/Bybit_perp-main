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
# Import the config module object and read attributes via getattr to avoid import-time failures
from . import config as cfg

logger = get_logger("scanner.scan")

# Read config values with safe defaults
EXCLUDE_STABLECOINS = getattr(cfg, "EXCLUDE_STABLECOINS", [])
CONCURRENCY = getattr(cfg, "CONCURRENCY", 4)
KLINE_SEED_LIMIT = getattr(cfg, "KLINE_SEED_LIMIT", 100)
ROOT_TFS = getattr(cfg, "ROOT_TFS", ["5", "15", "60", "240"])
MTF_TFS = getattr(cfg, "MTF_TFS", ["5", "15", "60", "240", "D"])
ROOT_SCAN_INTERVAL = getattr(cfg, "ROOT_SCAN_INTERVAL", 0)
TRADE_ENABLED = getattr(cfg, "TRADE_ENABLED", False)
MTF_SLOPE_LOOKBACK = getattr(cfg, "MTF_SLOPE_LOOKBACK", 3)
ROOT_FILTER = getattr(cfg, "ROOT_FILTER", False)
ROOT_TOP_N = getattr(cfg, "ROOT_TOP_N", None)
MAX_OPEN_TRADES = getattr(cfg, "MAX_OPEN_TRADES", 5)
USE_WS = getattr(cfg, "USE_WS", False)
MAX_CONCURRENT_REQUESTS = getattr(cfg, "MAX_CONCURRENT_REQUESTS", 8)
REQUEST_BATCH_SIZE = getattr(cfg, "REQUEST_BATCH_SIZE", 32)
REQUEST_BATCH_DELAY = getattr(cfg, "REQUEST_BATCH_DELAY", 0.1)
REST_POLL_INTERVAL = getattr(cfg, "REST_POLL_INTERVAL", 10)
VOLUME_FILTER_ENABLED = getattr(cfg, "VOLUME_FILTER_ENABLED", False)
VOLUME_MIN_CHANGE_PCT = getattr(cfg, "VOLUME_MIN_CHANGE_PCT", 0.15)
TECHNICAL_RATING = getattr(cfg, "TECHNICAL_RATING", {})
FLIP_CANDLE_AGE_MAX_SEC = getattr(cfg, "FLIP_CANDLE_AGE_MAX_SEC", 120)
SIGNAL_DEDUP_WINDOW = getattr(cfg, "SIGNAL_DEDUP_WINDOW", 30)
TRADE_RATING_MIN = getattr(cfg, "TRADE_RATING_MIN", 0)
TRADE_RATING_PRIORITIZE = getattr(cfg, "TRADE_RATING_PRIORITIZE", False)
# Optional config items with env fallback
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

# Telegram summary dispatch window (in seconds)
TELEGRAM_DISPATCH_WINDOW = 5


class ScannerScan:
    def __init__(self, client: BybitClient, rate_limiter: TokenBucket):
        self.rate_limiter = rate_limiter
        self.client = client
        self.trade_manager = None  # not owned here (kept in wrapper)
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

        # Track which ROOT_TF candles have opened in the current cycle
        self._last_tf_candle_open_times: Dict[str, float] = {tf: 0.0 for tf in ROOT_TFS}
        self._last_telegram_dispatch_time: Optional[float] = None

        # Signal deduplication cache: (symbol, tf, candle_open_time) -> signal_timestamp
        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        # Initialize Telegram state manager
        self.telegram = TelegramSummary()

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) "
            "TRADE_RATING_MIN=%s TV_RATING_WEIGHT=%.2f TRADE_RATING_PRIORITIZE=%s FLIP_CANDLE_AGE_MAX_SEC=%d SIGNAL_DEDUP_WINDOW=%d "
            "TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN, TV_RATING_WEIGHT, TRADE_RATING_PRIORITIZE, FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW,
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

    # Wrappers that use core helpers
    def _tf_to_seconds(self, tf: str) -> int:
        return tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str):
        return normalize_klines(raw_klines, tf)

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        # keep compatibility; actual quantize sits in scanner_core
        from .scanner_core import quantize_qty  # local import
        return quantize_qty(qty, step, min_qty)

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        """
        Build closes list from kline_store and call scanner_core.compute_macd_from_closes.
        include_price overwrites last close for immediate current-price MACD evaluation.
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

        return compute_macd_from_closes(closes, include_price=current_price)

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        return detect_flip_current_open(hist, hist_threshold)

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        """Update and track 24h volume data - tries multiple client method names and keys for robustness."""
        try:
            names = ["get_24h_ticker", "get24h", "get_24h", "get_ticker_24h", "ticker_24h", "get_ticker"]
            data = await self._call_client_method(names, symbol)
            if not data:
                logger.debug("[VOLUME_UPDATE] No ticker data returned for %s", symbol)
                return None

            # Normalize nested shapes
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
        return compute_24h_volume_change_from(self._24h_volumes.get(symbol))

    def compute_tv_rating(self, symbol: str, tf: str, price: Optional[float] = None):
        klines = self.kline_store.get(symbol, {}).get(tf, [])
        return compute_tv_rating_from(klines, TECHNICAL_RATING, tf=tf, price=price)

    def _compute_mtf_alignment(self, symbol: str, price: float):
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
        return compute_mtf_alignment(_get_closes, price, MTF_ALIGN_TFS, mtf_slope_lookback=MTF_SLOPE_LOOKBACK)

    def _compute_current_candle_start(self, tf: str, now: Optional[float] = None) -> int:
        """
        Compute canonical UTC epoch start time of the current candle for timeframe `tf`.
        This is used to ensure consistent deduplication and candle-age checks independent
        of kline_store contents (which can lag behind).
        """
        if now is None:
            now = time.time()
        try:
            tf_seconds = self._tf_to_seconds(tf)
            # daily or larger handled by day-alignment
            if tf in ("D",) or tf_seconds >= 86400:
                # start of the UTC day
                return int((int(now) // 86400) * 86400)
            # other TFs align straightforwardly
            return int((int(now) // tf_seconds) * tf_seconds)
        except Exception:
            # fallback: return integer seconds
            return int(now)

    def _detect_tf_candle_opens(self, now: float) -> List[str]:
        """
        Detect which ROOT_TFS have just opened a new candle.
        Returns list of ROOT_TFS that are within the first TELEGRAM_DISPATCH_WINDOW seconds.
        """
        opens = []
        now_struct = time.gmtime(now)
        current_hour = now_struct.tm_hour
        current_minute = now_struct.tm_min
        current_second = now_struct.tm_sec
        
        for tf in ROOT_TFS:
            try:
                tf_seconds = self._tf_to_seconds(tf)
                
                if tf in ("D", "W"):
                    if current_hour == 0 and current_minute == 0 and current_second < TELEGRAM_DISPATCH_WINDOW:
                        opens.append(tf)
                else:
                    if tf_seconds >= 3600:
                        candles_per_day = 86400 // tf_seconds
                        seconds_since_candle_open = (current_hour % (24 // candles_per_day)) * 3600 + current_minute * 60 + current_second
                        if seconds_since_candle_open < TELEGRAM_DISPATCH_WINDOW:
                            opens.append(tf)
                    else:
                        seconds_since_candle_open = (current_minute % (tf_seconds // 60)) * 60 + current_second
                        if seconds_since_candle_open < TELEGRAM_DISPATCH_WINDOW:
                            opens.append(tf)
            except Exception:
                logger.debug("Error detecting candle open for TF %s", tf, exc_info=True)
        
        return opens

    async def _check_monitored_symbols(self) -> List[Dict[str, Any]]:
        """Scenario B monitor: re-evaluate symbols waiting for their last negative TF to flip."""
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

    def _is_candle_age_acceptable(self, start_at: Optional[int], now: float) -> bool:
        """
        Check if a candle is fresh enough for trading.
        Returns True if:
        - FLIP_CANDLE_AGE_MAX_SEC is 0 (disabled, any age OK), or
        - candle age is within acceptable window
        """
        if FLIP_CANDLE_AGE_MAX_SEC <= 0:
            return True
        
        if start_at is None:
            logger.debug("Cannot check candle age: start_at is None")
            return False
        
        try:
            candle_age_sec = now - int(start_at)
            if candle_age_sec > FLIP_CANDLE_AGE_MAX_SEC:
                logger.debug("Candle too old for trading: age=%.0f sec (max=%.0f)", candle_age_sec, FLIP_CANDLE_AGE_MAX_SEC)
                return False
            return True
        except Exception as e:
            logger.debug("Error checking candle age: %s", e)
            return False

    def _try_dedupe_signal(self, symbol: str, tf: str, candle_open_time: Optional[int], now: float) -> bool:
        """
        Check if this (symbol, tf, candle_open_time) has already generated a signal recently.
        Returns True if signal is NEW (not in cache or cache expired); False if DUPLICATE.
        Caches the signal timestamp to prevent re-triggers across scan cycles.
        """
        if SIGNAL_DEDUP_WINDOW <= 0:
            # Deduplication disabled
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
            
            # Signal is new or cache expired; update cache
            self._signal_cache[cache_key] = now
            logger.debug("DEDUPE PASSED: %s %s candle_open=%d (new or expired)", symbol, tf, candle_open_time)
            return True
        except Exception as e:
            logger.debug("Error in dedupe check: %s", e)
            return True

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

                        # Ensure 24h volume is fully updated prior to signal generation
                        await self._update_24h_volume(sym)

                        now = time.time()

                        for root in ROOT_TFS:
                            logger.info("[ROOT_SCAN_CALC] %s %s: STARTING MACD calculation", sym, root)

                            macd_line, sig, hist = self.compute_macd_for(
                                sym,
                                root,
                                include_price=price,
                                use_ws_current=True
                            )

                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)

                            logger.info(
                                "[ROOT_SCAN_RESULT] %s %s: flip_detected=%s",
                                sym,
                                root,
                                flip
                            )

                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                start_at = None
                                try:
                                    # Prefer deterministic computed start time
                                    start_at = self._compute_current_candle_start(root, now=now)
                                except Exception:
                                    start_at = None

                                # Check if candle is fresh enough for trading
                                candle_age_ok = self._is_candle_age_acceptable(start_at, now)
                                
                                # Check if this signal was already generated in recent past (deduplication)
                                is_new_signal = self._try_dedupe_signal(sym, root, start_at, now)

                                if not candle_age_ok:
                                    logger.info("SIGNAL REJECTED (candle too old): %s %s @ %s", sym, root, price)
                                    continue
                                
                                if not is_new_signal:
                                    logger.info("SIGNAL REJECTED (duplicate): %s %s @ %s", sym, root, price)
                                    continue

                                tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                                
                                # Pre-compute MTF alignment so immediate blocks never show N/A status
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

                # Capture newly aligned monitored signals and evaluate them immediately
                newly_aligned = await self._check_monitored_symbols()
                evaluated_aligned = []
                if newly_aligned:
                    # Allow execution for monitored signals that are now fully aligned
                    await self._emit_event("root_signals_ready", {"root_signals": newly_aligned, "allow_open_trades": True})

                now_ts = time.time()
                is_full_push = self.telegram.check_full_push(now_ts)

                if root_signals and is_full_push:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))

                # Evaluate new candidates for trade manager execution
                if root_signals:
                    await self._emit_event("root_signals_ready", {"root_signals": root_signals, "allow_open_trades": is_full_push})

                # Dispatch Telegram summary messages
                evaluated_signals = []  # can be populated by listening to 'candidates_evaluated_result' event if desired

                if hasattr(self.telegram, "send_summary"):
                    try:
                        await self.telegram.send_summary(
                            root_signals=root_signals + newly_aligned,
                            evaluated=evaluated_signals,
                            full_push=is_full_push,
                            is_candle_open=is_full_push
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
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle", to_sleep)
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
        """
        Backwards compat local wrapper in scan module. By default, scan will emit signals to the composition layer
        which will call TradeEvaluator.handle_root_signals. This method kept for any internal usage.
        """
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
