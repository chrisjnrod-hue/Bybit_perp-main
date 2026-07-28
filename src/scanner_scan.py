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

# Telegram summary dispatch window (in seconds)
TELEGRAM_DISPATCH_WINDOW = 5

MIN_MACD_CANDLES = 26


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

        # Signal deduplication cache
        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        # Telegram summary state
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

    # ... seed_klines_for_symbol, seed_all, rest poller, etc. (same as previously provided)
    # Keep the implementations you already have. The important addition below is compute_24h_volume_change:

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
        """
        Synchronous helper expected by the TradeEvaluator wrapper.
        Returns the computed 24h volume change (or None) using the scanner's tracked _24h_volumes.
        """
        return compute_24h_volume_change_from(self._24h_volumes.get(symbol))

    # compute_macd_for is async in this file (as previously implemented)
    async def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        # (full implementation as in your deployed scanner_scan earlier)
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

        # Attempt fallback fetch when insufficient closes
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
                        self.kline_store.setdefault(symbol, {})[tf] = klines_sorted
                        closes = [float(x["close"]) for x in klines_sorted if x.get("close") is not None]
                        logger.debug("Fallback seed updated kline_store for %s %s: new_count=%d", symbol, tf, len(closes))
            except Exception:
                logger.exception("Fallback kline fetch failed for %s %s", symbol, tf)

        if not closes and current_price is not None:
            closes = [current_price]

        try:
            macd_line, signal_line, hist = compute_macd_from_closes(closes, include_price=current_price)
        except Exception:
            try:
                macd_line, signal_line, hist = compute_macd_from_closes([current_price] if current_price is not None else [], include_price=None)
            except Exception:
                macd_line, signal_line, hist = ([], [], [])
        return macd_line, signal_line, hist

    # (rest of the class methods: _compute_mtf_alignment, _compute_current_candle_start, dedupe, _detect_tf_candle_opens, _check_monitored_symbols, root_scan_loop, etc.)
    # Keep the implementations you already have. The two crucial helpers (async compute_macd_for and sync compute_24h_volume_change) are present above.

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
