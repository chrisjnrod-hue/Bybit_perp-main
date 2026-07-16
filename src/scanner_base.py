
# scanner_base.py
import os
import asyncio
import time
import json
import math
import inspect
from collections import defaultdict
from typing import Dict, List, Any, Optional, Callable
from decimal import getcontext

getcontext().prec = 28

from .logger import get_logger
from .bybit_client import BybitClient
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket
from .config import (
    EXCLUDE_STABLECOINS, CONCURRENCY, KLINE_SEED_LIMIT,
    ROOT_TFS, MTF_TFS, USE_WS,
    MAX_CONCURRENT_REQUESTS, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY,
    REST_POLL_INTERVAL, TECHNICAL_RATING, MTF_SLOPE_LOOKBACK
)

from .scanner_core import (
    tf_to_seconds, normalize_klines, quantize_qty, compute_macd_from_closes,
    detect_flip_current_open, compute_24h_volume_change_from,
    compute_tv_rating_from, compute_mtf_alignment
)

logger = get_logger("scanner_base")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]

class ScannerBase:
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

        # Telegram pushing/dedupe state
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}
        self._last_root_signal_send: Dict[tuple, float] = {}
        self._last_minimal_push_ts: Optional[float] = None

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

    def _tf_to_seconds(self, tf: str) -> int:
        return tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str):
        return normalize_klines(raw_klines, tf)

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        return quantize_qty(qty, step, min_qty)

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
