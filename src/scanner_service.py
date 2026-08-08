# scanner_service.py
# Core scanning, signal evaluation, and trade management orchestration.
# Telegram messaging delegated to scanner_telegram.py

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
    FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW, TRADE_RATING_MIN, TRADE_RATING_PRIORITIZE,
    ROOT_SEED_LIMIT, TARGETED_SEED_ENABLED, TRADE_OPEN_MODE
)
from .telegram import send_message

from .scanner_core import (
    tf_to_seconds,
    normalize_klines,
    quantize_qty,
    compute_macd_from_closes,
    detect_flip_current_open,
    compute_24h_volume_change_from,
    compute_tv_rating_from,
    compute_mtf_alignment,
    is_candle_age_acceptable,
)

from .scanner_telegram import TelegramSummary

logger = get_logger("scanner")

# Backwards compatibility for environment override of SEED_KLINES_LIMIT if not provided in config import
SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# Updated: Numeric TV rating threshold (replaces TRADE_RATING_ALLOW string-based filter)
try:
    TRADE_RATING_MIN_VAL = float(os.getenv("TRADE_RATING_MIN", str(TRADE_RATING_MIN)))
except (ValueError, TypeError):
    TRADE_RATING_MIN_VAL = TRADE_RATING_MIN

# TV rating weighting in combined score calculation (0.0 to 1.0, where 1.0 = 100% TV weight)
try:
    TV_RATING_WEIGHT = float(os.getenv("TV_RATING_WEIGHT", "0.3"))
    TV_RATING_WEIGHT = max(0.0, min(1.0, TV_RATING_WEIGHT))  # clamp to [0.0, 1.0]
except (ValueError, TypeError):
    TV_RATING_WEIGHT = 0.3

TRADE_NO_NEG_VOL = os.getenv("TRADE_NO_NEG_VOL", "1").strip().lower() in ("1", "true", "yes", "y")
MARKET_CAP_MIN = float(os.getenv("MARKET_CAP_MIN", "0") or 0)
PRIORITIZE_SLOT_ORDER = [p.strip() for p in os.getenv("PRIORITIZE_SLOT_ORDER", "240,D,60").split(",") if p.strip()]

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]

# Telegram summary dispatch window (in seconds) — group TF opens within this window
TELEGRAM_DISPATCH_WINDOW = 5

# Default config dictionary for TradeManager
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
        # FIX: Pass both exchange_client and config to TradeManager
        self.trade_manager = TradeManager(exchange_client=self.client, config=DEFAULT_TRADE_MANAGER_CONFIG)
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

        # Track which ROOT_TF candles have opened in the current cycle
        self._last_tf_candle_open_times: Dict[str, float] = {tf: 0.0 for tf in ROOT_TFS}
        self._last_telegram_dispatch_time: Optional[float] = None

        # Signal deduplication cache: (symbol, tf, candle_open_time) -> signal_timestamp
        # Prevents same candle from generating multiple signals across scan cycles
        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        # Initialize Telegram state manager
        self.telegram = TelegramSummary()

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) "
            "TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_RATING_PRIORITIZE=%s FLIP_CANDLE_AGE_MAX_SEC=%d SIGNAL_DEDUP_WINDOW=%d "
            "TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s TRADE_OPEN_MODE=%s TARGETED_SEED=%s ROOT_SEED_LIMIT=%s",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN_VAL, TV_RATING_WEIGHT, TRADE_RATING_PRIORITIZE, FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW,
            TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER, TRADE_OPEN_MODE, TARGETED_SEED_ENABLED, ROOT_SEED_LIMIT
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

    # New: fast root-only seeding helpers (targeted seeding)
    async def seed_root_for_symbol(self, symbol: str):
        """Fast seed only for ROOT_TFS for a single symbol (uses ROOT_SEED_LIMIT)."""
        try:
            tfs = list(set(ROOT_TFS))
            for tf in tfs:
                try:
                    async with self.request_sem:
                        raw = await self._call_get_klines(symbol, tf, limit=ROOT_SEED_LIMIT)
                    if not raw:
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
                    if valid:
                        try:
                            klines_sorted = sorted(valid, key=lambda x: x.get("start_at") or 0)
                        except Exception:
                            klines_sorted = valid
                        self.kline_store.setdefault(symbol, {})[tf] = klines_sorted
                except Exception:
                    logger.debug("seed_root_for_symbol: failed for %s %s", symbol, tf, exc_info=True)
        except Exception:
            logger.exception("seed_root_for_symbol failed for %s", symbol)

    async def seed_root_for_all(self):
        """Fast, parallel seeding for ROOT_TFS for all symbols. Intended to be lightweight."""
        if not self.symbols:
            return
        logger.info("[ROOT_SEED] Starting fast root-only seeding for %d symbols (limit=%d)", len(self.symbols), ROOT_SEED_LIMIT)
        sem = asyncio.Semaphore(max(1, CONCURRENCY))
        async def worker(s: str):
            async with sem:
                await self.seed_root_for_symbol(s)
        tasks = [asyncio.create_task(worker(s)) for s in self.symbols]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("[ROOT_SEED] Completed root-only seeding")

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
        return quantize_qty(qty, step, min_qty)

    def _get_current_candle_start(self, tf: str, now: float) -> int:
        tf_sec = self._tf_to_seconds(tf)
        if tf == "D":
            return int(now // 86400) * 86400
        elif tf == "W":
            return int(now // 604800) * 604800
        else:
            return int(now // tf_sec) * tf_sec

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        """
        Build closes list from kline_store and call core.compute_macd_from_closes.
        The optional use_ws_current path tries to consult the client's WS cached kline (best-effort).
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

    def _is_candle_age_acceptable(self, start_at: Optional[int], now: float) -> bool:
        """
        Check if a candle is fresh enough for trading using core function and FLIP_CANDLE_AGE_MAX_SEC config.
        """
        return is_candle_age_acceptable(start_at, now, FLIP_CANDLE_AGE_MAX_SEC)

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

    async def _wait_until_next_scan_boundary(self) -> float:
        """
        Wait until the next scan boundary (clock-aligned) and return the boundary timestamp.

        - If ROOT_SCAN_INTERVAL > 0: aligns to multiples of that interval from epoch (00:00 UTC).
        - If ROOT_SCAN_INTERVAL = 0: aligns to next 5-minute boundary (multiples of 300s: 00, 05, 10, ...).

        Returns:
            float: the epoch timestamp (seconds) of the boundary we woke for.
        """
        now = time.time()

        if ROOT_SCAN_INTERVAL and ROOT_SCAN_INTERVAL > 0:
            # Align to multiples of ROOT_SCAN_INTERVAL from epoch (exact)
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
            # ROOT_SCAN_INTERVAL = 0: align to next 5-minute boundary (multiples of 300 seconds)
            FIVE_MIN = 300
            next_boundary = ((int(now) // FIVE_MIN) + 1) * FIVE_MIN
            to_sleep = next_boundary - now
            # Ensure non-negative sleep
            to_sleep = max(0.0, to_sleep)
            # Log target in human readable terms
            next_struct = time.gmtime(next_boundary)
            logger.info(
                "[SCAN_BOUNDARY] ROOT_SCAN_INTERVAL=0: aligning to next 5m boundary at %02d:%02d:%02d UTC (sleeping %.3f sec)",
                next_struct.tm_hour, next_struct.tm_min, next_struct.tm_sec, to_sleep
            )
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)
            return float(next_boundary)

    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1

            # ===== SYNC TO NEXT SCAN BOUNDARY (BEFORE ANY WORK) =====
            boundary_ts = await self._wait_until_next_scan_boundary()

            logger.info("[DIAGNOSTIC] root_scan_loop: Beginning scan cycle #%d at boundary_ts=%d", loop_count, int(boundary_ts))

            start = time.time()
            try:
                if not self.symbols:
                    logger.info("[DIAGNOSTIC] root_scan_loop: No symbols, discovering...")
                    await self.discover_symbols()
                    if self.symbols:
                        logger.info("[DIAGNOSTIC] root_scan_loop: Starting symbol seed (count=%d)", len(self.symbols))
                        # initial full seed in background to avoid blocking on startup
                        try:
                            asyncio.create_task(self.seed_all())
                        except Exception:
                            logger.exception("Failed to schedule initial seed_all()")
                        # do a fast root-only seed before first run
                        if TARGETED_SEED_ENABLED:
                            try:
                                await self.seed_root_for_all()
                            except Exception:
                                logger.exception("Initial fast root-only seeding failed")
                        logger.info("[DIAGNOSTIC] root_scan_loop: Symbol seeding initiated")
                    else:
                        logger.warning("[DIAGNOSTIC] root_scan_loop: Symbol discovery returned empty!")
                        await asyncio.sleep(10)
                        continue

                await self._ensure_rest_poller()

                # ---- ROOT TF CANDLE OPEN DETECTION & SEEDING/REFRESH ----
                now = float(boundary_ts)  # use exact boundary timestamp for open detection
                refreshed_tfs = []
                for root in ROOT_TFS:
                    current_start = self._get_current_candle_start(root, now)
                    last_start = self._last_tf_candle_open_times.get(root, 0.0)

                    if current_start > last_start:
                        logger.info("[CANDLE_OPEN] New candle opened for root TF %s at timestamp %d (previous was %d)", root, current_start, last_start)
                        self._last_tf_candle_open_times[root] = float(current_start)
                        refreshed_tfs.append(root)

                if refreshed_tfs:
                    logger.info(
                        "[CANDLE_OPEN_REFRESH] Detected new candle open(s) for root TFs %s. Triggering background full seed and doing fast root-only seed.",
                        refreshed_tfs
                    )
                    # FIRE full seeding in background so the main scan path is not blocked
                    try:
                        asyncio.create_task(self.seed_all())
                    except Exception:
                        logger.exception("Failed to schedule background seed_all()")
                    # Do a fast root-only seed synchronously (targeted) to ensure root-TF detection is quick.
                    if TARGETED_SEED_ENABLED:
                        try:
                            await self.seed_root_for_all()
                        except Exception:
                            logger.exception("Fast root-only seeding failed")
                    logger.info("[CANDLE_OPEN_REFRESH] Background full seed scheduled; root-only seed attempted.")
                # -------------------------------------------------------------

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

                        now_check = time.time()

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
                                    last_candles = self.kline_store.get(sym, {}).get(root, [])
                                    if last_candles:
                                        start_at = last_candles[-1].get("start_at")
                                except Exception:
                                    start_at = None

                                # Check if candle is fresh enough for trading
                                candle_age_ok = self._is_candle_age_acceptable(start_at, now_check)

                                # Check if this signal was already generated in recent past (deduplication)
                                is_new_signal = self._try_dedupe_signal(sym, root, start_at, now_check)

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
                                    "candle_age_ok": candle_age_ok,
                                    "mtf_status": mtf_align.get("status", "N/A"),
                                    "negative_tfs": mtf_align.get("negative_tfs", []),
                                    "score": sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip")) + (min(vol_change, 1.0) if vol_change is not None and vol_change > 0 else 0.0)
                                }
                                root_signals.append(sig_item)
                                if not candle_age_ok:
                                    logger.info("SIGNAL DETECTED (OLD CANDLE - TELEGRAM ONLY): %s %s @ %s (tv=%s %+.3f candle_age=%.0f sec)", sym, root, price, tv_label, tv_score, now_check - start_at if start_at else -1)
                                else:
                                    logger.info("SIGNAL DETECTED: %s %s @ %s (tv=%s %+.3f candle_age=%.0f sec)", sym, root, price, tv_label, tv_score, now_check - start_at if start_at else -1)

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
                    evaluated_aligned = await self.handle_root_signals(newly_aligned, allow_open_trades=True)

                # Use the exact boundary timestamp for Telegram full-push decision so pushes are tied to the aligned boundary
                now_ts = float(boundary_ts)
                is_full_push = self.telegram.check_full_push(now_ts)

                if root_signals and is_full_push:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))

                # Decide whether we allow opens now depending on TRADE_OPEN_MODE:
                allow_opens_now = is_full_push
                if TRADE_OPEN_MODE == "immediate":
                    allow_opens_now = True

                # Evaluate new candidates for trade manager execution
                evaluated_signals = []
                if root_signals:
                    evaluated_signals = await self.handle_root_signals(root_signals, allow_open_trades=allow_opens_now)

                # Combine both new signals and resolved monitoring signals for the Telegram summary
                if newly_aligned:
                    root_signals.extend(newly_aligned)
                    evaluated_signals.extend(evaluated_aligned)

                # Dispatch Telegram summary messages
                if hasattr(self.telegram, "send_summary"):
                    try:
                        await self.telegram.send_summary(
                            root_signals=root_signals,
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

    # New: on-demand MTF/TV fetch for candidate(s)
    async def _fetch_mtf_and_tv_for(self, candidate: Dict[str, Any], need_tfs: List[str] = None):
        """
        Fetch MTF TF klines and TV rating for a single candidate and update candidate in-place.
        This respects request_sem and is best-effort (does not throw on failures).
        """
        try:
            sym = candidate["symbol"]
            price = candidate.get("price")
            tfs = need_tfs or list(set(MTF_TFS))
            # fetch MTF TF klines concurrently (limited by request_sem)
            async def _get_for_tf(tf):
                try:
                    async with self.request_sem:
                        raw = await self._call_get_klines(sym, tf, limit=SEED_KLINES_LIMIT)
                    normalized = normalize_klines(raw, tf) if raw else []
                    self.kline_store.setdefault(sym, {})[tf] = normalized
                    return tf, normalized
                except Exception:
                    logger.debug("MTF fetch failed for %s %s", sym, tf, exc_info=True)
                    return tf, []
            tasks = [asyncio.create_task(_get_for_tf(tf)) for tf in tfs]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # compute tv rating and mtf alignment using updated kline_store
            try:
                tv_score, tv_label = self.compute_tv_rating(sym, candidate.get("root"), price)
            except Exception:
                tv_score, tv_label = 0.0, "Neutral"
            try:
                mtf_align = self._compute_mtf_alignment(sym, price)
            except Exception:
                mtf_align = {"status": "N/A", "tfs": {}, "negative_tfs": []}
            candidate["tv_score"] = tv_score
            candidate["tv_label"] = tv_label
            candidate["mtf_status"] = mtf_align.get("status", "N/A")
            candidate["negative_tfs"] = mtf_align.get("negative_tfs", [])
            # recompute candidate base score (mtf_score + vol contribution) to be used for sorting
            try:
                vol_change = candidate.get("vol_change")
                score = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))
                if vol_change is not None and vol_change > 0:
                    score += min(vol_change, 1.0)
                candidate["score"] = score
            except Exception:
                pass
        except Exception:
            logger.exception("fetch mtf/tv failed for %s", candidate.get("symbol"))

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
                # ---- CANDLE AGE FILTER: Reject/block trade open if candle too old, but allow Telegram push ----
                if not candle_age_ok:
                    entry["accept"] = False
                    entry["reason"] = "candle_too_old"
                    logger.info("Trade blocked by FLIP_CANDLE_AGE_MAX_SEC (candle too old): %s root=%s", sym, root)
                    evaluated.append(entry)
                    continue

                # ---- TV RATING FILTER: Reject if below TRADE_RATING_MIN ----
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

        # ---- PRIORITIZE BY TV RATING ----
        if TRADE_RATING_PRIORITIZE and candidates:
            candidates = sorted(candidates, key=lambda c: c.get("tv_score", 0.0), reverse=True)
            logger.info("Sorted %d candidates by TV rating (highest first)", len(candidates))

        # Selection & slot allocation (ROOT_FILTER / PRIORITIZE_SLOT_ORDER handling)
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

        # --- PRE-FETCH MTF/TV FOR CANDIDATES BASED ON TRADE_OPEN_MODE ---
        available_slots = max(0, MAX_OPEN_TRADES - (len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0))
        if TRADE_OPEN_MODE == "immediate":
            logger.debug("TRADE_OPEN_MODE=immediate: skipping MTF/TV prefetch - open on first-come filters")
            # no prefetch
            pass
        elif TRADE_OPEN_MODE == "balanced" and candidates:
            shortlist_n = max(1, min(len(candidates), (available_slots * 2) or 1))
            shortlist = candidates[:shortlist_n]
            logger.info("TRADE_OPEN_MODE=balanced: prefetching MTF/TV for top %d candidates", shortlist_n)
            tasks = [asyncio.create_task(self._fetch_mtf_and_tv_for(c)) for c in shortlist]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # merge updated shortlist back
            for i, c in enumerate(candidates):
                for s in shortlist:
                    if s["symbol"] == c["symbol"] and s["root"] == c["root"]:
                        candidates[i] = s
            candidates = sorted(candidates, key=lambda r: self._compute_combined_score(r), reverse=True)
        elif TRADE_OPEN_MODE == "conservative" and candidates:
            logger.info("TRADE_OPEN_MODE=conservative: fetching MTF/TV for all %d candidates before opening", len(candidates))
            tasks = [asyncio.create_task(self._fetch_mtf_and_tv_for(c)) for c in candidates]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
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
