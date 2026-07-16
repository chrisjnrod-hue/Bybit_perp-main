# scanner_service.py
# Full updated Scanner class with gating, volume fixes, prioritization, TV rating numeric threshold,
# corrected Telegram notification order, duplicate-trade prevention, and epoch modulo sleep alignment.
# Safeguard implemented: is_new_candle logic for MACD.

import os
import asyncio
import time
import json
from collections import defaultdict
from typing import Dict, List, Any, Optional, Callable
import math
import inspect
from decimal import getcontext

getcontext().prec = 28

from .logger import get_logger
from .bybit_client import BybitClient
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket
from .macd import slope
from .config import (
    EXCLUDE_STABLECOINS, CONCURRENCY, KLINE_SEED_LIMIT,
    ROOT_TFS, MTF_TFS, ROOT_SCAN_INTERVAL, TRADE_ENABLED,
    MTF_SLOPE_LOOKBACK, ROOT_FILTER, ROOT_TOP_N, MAX_OPEN_TRADES, USE_WS,
    MAX_CONCURRENT_REQUESTS, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY,
    REST_POLL_INTERVAL, VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT, TECHNICAL_RATING
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
)

logger = get_logger("scanner")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

try:
    TRADE_RATING_MIN = float(os.getenv("TRADE_RATING_MIN", "0.25"))
except (ValueError, TypeError):
    TRADE_RATING_MIN = 0.25

try:
    TV_RATING_WEIGHT = float(os.getenv("TV_RATING_WEIGHT", "0.3"))
    TV_RATING_WEIGHT = max(0.0, min(1.0, TV_RATING_WEIGHT))
except (ValueError, TypeError):
    TV_RATING_WEIGHT = 0.3

TRADE_NO_NEG_VOL = os.getenv("TRADE_NO_NEG_VOL", "1").strip().lower() in ("1", "true", "yes", "y")
MARKET_CAP_MIN = float(os.getenv("MARKET_CAP_MIN", "0") or 0)
PRIORITIZE_SLOT_ORDER = [p.strip() for p in os.getenv("PRIORITIZE_SLOT_ORDER", "240,D,60").split(",") if p.strip()]

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

        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}
        self._last_root_signal_send: Dict[tuple, float] = {}
        self._last_minimal_push_ts: Optional[float] = None

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN, TV_RATING_WEIGHT, TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER
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

    def _tf_to_seconds(self, tf: str) -> int:
        return tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str):
        return normalize_klines(raw_klines, tf)

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        return quantize_qty(qty, step, min_qty)

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False, is_new_candle: bool = False):
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

        return compute_macd_from_closes(closes, include_price=current_price, is_new_candle=is_new_candle)

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        return detect_flip_current_open(hist, hist_threshold)

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        try:
            names = ["get_24h_ticker", "get24h", "get_24h", "get_ticker_24h", "ticker_24h", "get_ticker"]
            data = await self._call_client_method(names, symbol)
            if not data:
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
                            vol = None

            if vol is not None:
                if symbol not in self._24h_volumes:
                    self._24h_volumes[symbol] = {"current": vol, "previous": vol}
                else:
                    self._24h_volumes[symbol]["previous"] = self._24h_volumes[symbol]["current"]
                    self._24h_volumes[symbol]["current"] = vol
                return vol
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

    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1
            start = time.time()
            try:
                if not self.symbols:
                    await self.discover_symbols()
                    if self.symbols:
                        await self.seed_all()
                    else:
                        await asyncio.sleep(10)
                        continue

                await self._ensure_rest_poller()
                root_signals: List[Dict[str, Any]] = []

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

                        for root in ROOT_TFS:
                            # SAFEGUARD LOGIC: determine if we are starting a new candle
                            last_candles = self.kline_store.get(sym, {}).get(root, [])
                            is_new_candle = False
                            if last_candles:
                                tf_seconds = self._tf_to_seconds(root)
                                if tf_seconds > 0:
                                    current_candle_start = (int(time.time()) // tf_seconds) * tf_seconds
                                    if current_candle_start > last_candles[-1].get("start_at", 0):
                                        is_new_candle = True

                            macd_line, sig, hist = self.compute_macd_for(
                                sym,
                                root,
                                include_price=price,
                                use_ws_current=True,
                                is_new_candle=is_new_candle
                            )

                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)

                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                start_at = last_candles[-1].get("start_at") if last_candles else None
                                tv_score, tv_label = self.compute_tv_rating(sym, root, price)

                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change,
                                    "start_at": start_at,
                                    "tv_score": tv_score,
                                    "tv_label": tv_label
                                })
                    except Exception:
                        logger.exception("Error checking symbol %s", sym)

                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(check_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks)
                    if i + REQUEST_BATCH_SIZE < len(self.symbols):
                        await asyncio.sleep(REQUEST_BATCH_DELAY)

                await self._check_monitored_symbols()
                full_push = False
                now_ts = time.time()
                if self._first_deploy_push:
                    full_push = True
                    for rt in ROOT_TFS:
                        tf_seconds = self._tf_to_seconds(rt)
                        if tf_seconds and tf_seconds > 0:
                            self._last_full_push_ts[rt] = (int(now_ts) // tf_seconds) * tf_seconds
                else:
                    for rt in ROOT_TFS:
                        tf_seconds = self._tf_to_seconds(rt)
                        if tf_seconds and tf_seconds > 0:
                            candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                            if self._last_full_push_ts.get(rt) != candle_start:
                                full_push = True
                                self._last_full_push_ts[rt] = candle_start

                minimal_allowed = (full_push or self._first_deploy_push)
                if not minimal_allowed and ROOT_SCAN_INTERVAL:
                     if self._last_minimal_push_ts is None or (now_ts - self._last_minimal_push_ts) >= max(1, float(ROOT_SCAN_INTERVAL)):
                         minimal_allowed = True

                evaluated = []
                if root_signals:
                    if minimal_allowed or full_push:
                        for sig in root_signals:
                            try:
                                sym = sig["symbol"]
                                if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                    await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                            except Exception:
                                logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))
                    evaluated = await self.handle_root_signals(root_signals, allow_open_trades=(minimal_allowed or full_push))

                await self.send_summary(root_signals, evaluated=evaluated, full_push=full_push)
                if self._first_deploy_push and full_push:
                    self._first_deploy_push = False

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start
            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                await asyncio.sleep(to_sleep)
            else:
                now = time.time()
                to_sleep = (300 - (now % 300)) + 0.5
                await asyncio.sleep(to_sleep)

    async def _check_monitored_symbols(self):
        if not self._mtf_monitoring: return
        MONITORING_MAX_AGE = 86400
        now = time.time()
        to_remove = []
        newly_aligned = []
        alert_blocks = []

        for sym, info in list(self._mtf_monitoring.items()):
            try:
                if now - info.get("started_at", now) > MONITORING_MAX_AGE:
                    to_remove.append(sym); continue
                price = self._last_price_cache.get(sym)
                if price is None:
                    async with self.request_sem:
                        price = await self.client.get_latest_price(sym)
                    if price: self._last_price_cache[sym] = price
                if price is None: continue

                mtf_align = self._compute_mtf_alignment(sym, price)
                status = mtf_align["status"]
                root = info.get("root", "?")

                if status in ("aligned", "daily_rising"):
                    to_remove.append(sym)
                    vol_change = self.compute_24h_volume_change(sym)
                    tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                    newly_aligned.append({"symbol": sym, "root": info["root"], "price": price, "hist": [], "vol_change": vol_change, "tv_score": tv_score, "tv_label": tv_label, "from_monitoring": True})
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
        for block in alert_blocks:
            await send_message(block)
        if newly_aligned:
            await self.handle_root_signals(newly_aligned, allow_open_trades=True)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]], allow_open_trades: bool = True) -> List[Dict[str, Any]]:
        evaluated = []
        to_open = []
        open_symbols = []
        if hasattr(self.trade_manager, "open_trades"):
            if isinstance(self.trade_manager.open_trades, dict):
                open_symbols = list(self.trade_manager.open_trades.keys())
            elif isinstance(self.trade_manager.open_trades, list):
                open_symbols = [t.get("symbol") for t in self.trade_manager.open_trades if isinstance(t, dict)]

        for item in root_signals:
            sym, price, root = item["symbol"], item["price"], item["root"]
            if sym in open_symbols:
                evaluated.append({"symbol": sym, "root": root, "price": price, "accept": False, "reason": "already_open"})
                continue

            hist = item.get("hist", [])
            if not hist:
                _, _, hist = self.compute_macd_for(sym, root, include_price=price)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            mtf_align = self._compute_mtf_alignment(sym, price)
            mtf_status, negative_tfs = mtf_align["status"], mtf_align.get("negative_tfs", [])
            score = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive"))
            score += sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))
            vol_change = item.get("vol_change")
            if vol_change and vol_change > 0: score += min(vol_change, 1.0)

            entry = {"symbol": sym, "root": root, "price": price, "hist": hist, "macd_hist_val": macd_hist_val, "mtf": mtf_align["tfs"], "mtf_status": mtf_status, "negative_tfs": negative_tfs, "vol_change": vol_change, "score": score, "accept": False, "reason": "pending", "tv_label": item.get("tv_label"), "tv_score": item.get("tv_score", 0.0)}

            if mtf_status in ("aligned", "daily_rising"):
                if entry["tv_score"] < TRADE_RATING_MIN:
                    entry["reason"] = "tv_rating_below_threshold"
                elif VOLUME_FILTER_ENABLED and (vol_change is None or vol_change < VOLUME_MIN_CHANGE_PCT):
                    entry["reason"] = "vol_filter_blocked"
                elif TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                    entry["reason"] = "negvol_blocked"
                else:
                    entry["accept"] = True
                    entry["reason"] = mtf_status
                    to_open.append(entry)
            elif mtf_status == "monitoring":
                entry["reason"] = "monitoring"
                if sym not in self._mtf_monitoring:
                    self._mtf_monitoring[sym] = {"root": root, "price": price, "started_at": time.time(), "negative_tfs": list(negative_tfs), "last_alert": 0.0}
            evaluated.append(entry)

        await self._emit_event("candidates_evaluated", evaluated)
        candidates = to_open
        
        # Sort/Filter
        candidates = sorted(candidates, key=lambda r: self._compute_combined_score(r), reverse=True)
        
        if not allow_open_trades:
            return evaluated

        for c in candidates:
            if not self.trade_manager.can_open(): break
            sym, price = c["symbol"], c["price"]
            try:
                balance = await self.client.get_balance("USDT")
                symbol_info = await self.client.get_symbol_info(sym)
                qty = self._quantize_qty(self.trade_manager.compute_qty_from_balance(balance, price, symbol_info), symbol_info.get("step"), symbol_info.get("min_qty"))
                if qty <= 0: continue
                
                if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                    order = await self.client.create_order(sym, "Buy", qty)
                    self.trade_manager.open_trade(sym, "Buy", price, qty, {"order": order})
                else:
                    self.trade_manager.open_trade(sym, "Buy", price, qty, {"simulated": True})
            except Exception:
                logger.exception("Failed to open trade for %s", sym)
            self._mtf_monitoring.pop(sym, None)
        return evaluated

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        return (candidate.get("score", 0.0) * (1.0 - TV_RATING_WEIGHT)) + (candidate.get("tv_score", 0.0) * TV_RATING_WEIGHT)

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None, full_push: bool = False):
        now_ts = time.time()
        eval_map = {(e["symbol"], e["root"]): e for e in (evaluated or [])}
        if full_push:
            self._last_minimal_push_ts = None
            try:
                accepted = [e for e in (evaluated or []) if e.get("accept")]
                recommended = sorted(accepted, key=self._compute_combined_score, reverse=True)[:MAX_OPEN_TRADES]
                lines = [f"🏆 Recommended Signals – {time.strftime('%H:%M UTC')}"]
                for r in recommended:
                    lines.append(f"  - {r['symbol']} | {r['root']} | ${r['price']:.4f} | combined={self._compute_combined_score(r):.2f}")
                await send_message("\n".join(lines))
            except Exception: pass
            
            for sig in root_signals:
                try:
                    sym, rt = sig["symbol"], sig["root"]
                    eval_entry = eval_map.get((sym, rt), {})
                    mtf_status = eval_entry.get("mtf_status", "N/A")
                    await send_message(f"📌 Bybit Perp | {rt} Signal\nSymbol: {sym}\nPrice: ${sig['price']:.4f}\nMTF Status: {mtf_status}\nTV: {sig['tv_label']} ({sig['tv_score']:+.3f})")
                    self._last_root_signal_send[(sym, rt)] = now_ts
                except Exception: pass
        else:
            for sig in root_signals:
                sym, rt = sig["symbol"], sig["root"]
                if (sym, rt) not in self._last_root_signal_send:
                    try:
                        await send_message(f"📌 Bybit Perp | {rt} Signal\nSymbol: {sym}\nPrice: ${sig['price']:.4f}\nTV: {sig['tv_label']} ({sig['tv_score']:+.3f})")
                        self._last_root_signal_send[(sym, rt)] = now_ts
                        self._last_minimal_push_ts = now_ts
                    except Exception: pass

    async def run(self):
        self._task = asyncio.create_task(self.root_scan_loop())
        try:
            await self._task
        except asyncio.CancelledError:
            logger.info("Scanner run cancelled")
        finally:
            try: await self.client.close()
            except Exception: pass

    def stop(self):
        self._stop = True
        if self._task and not self._task.done(): self._task.cancel()
        if self._rest_poller_task and not self._rest_poller_task.done():
            try: self._rest_poller_task.cancel()
            except: pass
