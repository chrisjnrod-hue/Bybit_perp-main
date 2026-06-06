# scanner_core.py
# Diagnostic + MTF alignment updated version (split-out core)
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
    MTF_SLOPE_LOOKBACK, MAX_OPEN_TRADES, USE_WS, MACD_HIST_THRESHOLD
)
from .telegram import send_message
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket

getcontext().prec = 28
logger = get_logger("scanner_core")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
REST_POLL_INTERVAL = int(os.getenv("REST_POLL_INTERVAL", "5"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "3"))
REQUEST_BATCH_SIZE = int(os.getenv("REQUEST_BATCH_SIZE", "5"))
REQUEST_BATCH_DELAY = float(os.getenv("REQUEST_BATCH_DELAY", "0.5"))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

# explicit MTF order we will scan (5m,15m,1h,4h,1d) - ensure these are present
DEFAULT_MTF_ORDER = ["5", "15", "60", "240", "1d"]


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
        # watchlist: symbols with at least one negative MTF that we are monitoring until flip
        self._watchlist: Dict[str, Dict[str, Any]] = {}
        # deferred openings: candidates detected on previous cycle that will be executed now if allowed
        self._deferred_candidates: List[Dict[str, Any]] = []
        # ensure we use configured MTF order (fallback)
        self.mtf_order = MTF_TFS if MTF_TFS else DEFAULT_MTF_ORDER
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

    # -------------- symbols fetching (unchanged logic) --------------
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

    # -------------- kline normalization / seeding --------------
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

        return out

    async def seed_klines_for_symbol(self, symbol: str):
        if SEED_KLINES_LIMIT < 100:
            logger.warning("SEED_KLINES_LIMIT is low (%d); consider >=200", SEED_KLINES_LIMIT)
        tfs = list(set(ROOT_TFS + self.mtf_order))
        for tf in tfs:
            try:
                async with self.request_sem:
                    raw = await self._call_get_klines(symbol, tf, limit=SEED_KLINES_LIMIT)

                if not raw:
                    continue

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

                if not valid:
                    continue

                try:
                    klines_sorted = sorted(valid, key=lambda x: x.get("start_at") or 0)
                except Exception:
                    klines_sorted = valid
                self.kline_store[symbol][tf] = klines_sorted
                await self._emit_event("klines_seeded", {"symbol": symbol, "tf": tf, "count": len(klines_sorted)})
            except Exception:
                logger.exception("Seed klines failed for %s %s", symbol, tf)

    async def seed_all(self):
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

    # -------------- REST poller (unchanged behavior, keeps kline_store up) --------------
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

    # -------------- MACD helpers --------------
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
        try:
            hist = [None if v is None else float(v) for v in (hist or [])]
        except Exception:
            pass
        return macd_line, signal_line, hist

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        if not hist or len(hist) < 2:
            return False
        prev = hist[-2]
        cur = hist[-1]
        if prev is None or cur is None:
            return False
        try:
            zero_cross = prev <= 0 and cur > 0
            return zero_cross
        except Exception:
            return False

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        if qty is None:
            return 0.0
        qty_d = Decimal(str(qty))
        if step is None or step <= 0:
            if min_qty and qty_d < Decimal(str(min_qty)):
                return float(Decimal(str(min_qty)))
            return float(qty_d)
        step_d = Decimal(str(step))
        mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
        quant = (mult * step_d)
        if min_qty is not None:
            min_d = Decimal(str(min_qty))
            if quant < min_d:
                quant = min_d
        try:
            quant = quant.normalize()
        except Exception:
            pass
        return float(quant)

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
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

    # -------------- Watchlist handling --------------
    def _add_to_watchlist(self, symbol: str, info: Dict[str, Any]):
        self._watchlist[symbol] = {"added_at": time.time(), "info": info}

    def _remove_from_watchlist(self, symbol: str):
        if symbol in self._watchlist:
            del self._watchlist[symbol]

    async def _evaluate_watchlist(self):
        """
        Check watchlist symbols for flips on monitored MTFs; return list of promoted candidates.
        """
        promoted = []
        if not self._watchlist:
            return promoted
        for sym, data in list(self._watchlist.items()):
            try:
                price = self._last_price_cache.get(sym)
                if price is None:
                    price = await self.client.get_latest_price(sym)
                    if price is None:
                        continue
                # check MTFs for flip condition (prefer last tf flips)
                for tf in self.mtf_order:
                    _, _, hist = self.compute_macd_for(sym, tf, include_price=price, use_ws_current=True)
                    flipped = self.detect_flip_current_open(hist or [], 0.0, symbol=sym, tf=tf)
                    cur_hist = hist[-1] if hist and len(hist) >= 1 else None
                    # if flipped or now positive, promote
                    if flipped or (cur_hist is not None and cur_hist > 0):
                        logger.info("Watchlist promoted %s due to %s flip/positive", sym, tf)
                        promoted.append({"symbol": sym, "price": price})
                        self._remove_from_watchlist(sym)
                        break
            except Exception:
                logger.exception("Error evaluating watchlist symbol %s", sym)
        return promoted

    # -------------- root scan loop core --------------
    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        first_cycle = True
        loop_count = 0

        while not self._stop:
            loop_count += 1
            logger.info("[DIAGNOSTIC] root_scan_loop: Beginning scan cycle #%d", loop_count)
            start = time.time()

            try:
                # 1) Deferred candidate processing (if we had detected previously and TRADE_ENABLED requires waiting)
                if self._deferred_candidates:
                    logger.info("Processing %d deferred candidates (attempt opens now)", len(self._deferred_candidates))
                    for cand in list(self._deferred_candidates):
                        try:
                            await self._attempt_open_from_candidate(cand)
                        except Exception:
                            logger.exception("Error processing deferred candidate %s", cand)
                        finally:
                            try:
                                self._deferred_candidates.remove(cand)
                            except Exception:
                                pass

                # 2) Promote watchlist entries if they flipped
                promoted = await self._evaluate_watchlist()
                root_signals: List[Dict[str, Any]] = []
                # If any promotions occurred, convert them into root_signals (with default root '1h')
                for p in promoted:
                    root_signals.append({
                        "symbol": p["symbol"],
                        "root": ROOT_TFS[0] if ROOT_TFS else "60",
                        "price": p["price"],
                        "hist": [],
                        "vol_change": self.compute_24h_volume_change(p["symbol"])
                    })

                # 3) Ensure we have symbols; discover and seed if needed
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

                # 4) Main symbol checks
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
                            macd_line, sig, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)

                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)

                            logger.debug("[ROOT_SCAN_RESULT] %s %s: flip_detected=%s last_hist=%s", sym, root, flip, hist[-1] if hist else None)

                            if hist and (flip or (hist[-1] is not None and hist[-1] > MACD_HIST_THRESHOLD)):
                                vol_change = self.compute_24h_volume_change(sym)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change
                                })
                                logger.info("SIGNAL DETECTED: %s %s @ %s", sym, root, price)
                    except Exception:
                        logger.exception("Error checking symbol %s", sym)

                # batch-run checks
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

                logger.info("[DIAGNOSTIC] root_scan_loop: Checked %d symbols, found %d root_signals", checked_count, len(root_signals))

                # always emit root_signals event & send summary (first cycle also)
                await self._emit_event("root_signals", root_signals)
                await self.send_summary(root_signals)

                # 5) If we detected root_signals, handle them (evaluate MTF alignment & open logic)
                if root_signals:
                    # For each detected root signal evaluate MTF alignment according to your rules
                    await self.handle_root_signals(root_signals)
                else:
                    logger.info("No root signals this interval.")

                # 6) 5-min-before-hour auto-close logic: if exactly max open trades, close least profitable
                await self._maybe_close_before_hour()

                # set flag that first cycle completed
                if first_cycle:
                    first_cycle = False

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start
            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                # run aligned to 5m candle opens (nearest future 5m boundary)
                now = time.time()
                next_5m = math.ceil(now / 300.0) * 300.0
                to_sleep = max(0, next_5m - now)
                logger.debug("ROOT_SCAN_INTERVAL not set; sleeping until next 5m open in %.1fs", to_sleep)
                await asyncio.sleep(to_sleep)

    async def _attempt_open_from_candidate(self, c: Dict[str, Any]):
        """
        Attempt to open a trade for candidate c. Uses trade_manager and client.
        """
        sym = c["symbol"]
        price = c["price"]
        try:
            balance = await self.client.get_balance("USDT")
        except Exception:
            balance = None
        symbol_info = await self.client.get_symbol_info(sym)
        qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
        qty = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
        if qty <= 0 or math.isclose(qty, 0.0):
            logger.warning("Computed qty for %s was zero after quantize (qty=%s). Skipping open.", sym, qty)
            return
        side = "Buy"
        if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
            try:
                order = await self.client.create_order(sym, side, qty)
                self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                await send_message(f"Opened trade {sym} {side} @ {price} qty={qty:.6f}")
            except Exception:
                logger.exception("Failed to place order for %s", sym)
        else:
            t = self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True})
            logger.info("Simulated open %s qty=%s", sym, qty)
            await send_message(f"Simulated open {sym} {side} @ {price} qty={qty:.6f}")

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]):
        """
        New MTF alignment logic (per your spec):
        - For each root signal, scan MTF_TFS in order (5m,15m,60,240,1d).
        - A: If any MTF TF has positive MACD (hist>0) OR current flip, accept candidate.
        - B: If one or more TF is negative, add to watchlist until flip; do not open now.
        - C: If only 1d TF negative but its slope is positive, accept.
        - If TRADE_ENABLED is True: defer actual opens to next scan cycle (store in _deferred_candidates).
        """
        evaluated = []
        for item in root_signals:
            sym = item["symbol"]
            price = item["price"]
            mtf_state = {}
            positive_any = False
            negative_count = 0
            any_positive_flip = False
            only_1d_negative = True
            one_d_slope_val = None

            for tf in self.mtf_order:
                macd_line, sig, h = self.compute_macd_for(sym, tf, include_price=price, use_ws_current=True)
                cur_hist = h[-1] if h and len(h) >= 1 else None
                prev_hist = h[-2] if h and len(h) >= 2 else None
                flipped = self.detect_flip_current_open(h or [], 0.0, symbol=sym, tf=tf)
                mtf_state[tf] = {"prev": prev_hist, "cur": cur_hist, "flip": flipped}

                if cur_hist is not None and cur_hist > 0:
                    positive_any = True
                if flipped:
                    any_positive_flip = True
                if cur_hist is not None and cur_hist < 0:
                    negative_count += 1
                if tf != "1d" and (cur_hist is None or cur_hist >= 0):
                    # if any non-1d tf not negative, it's not "only 1d negative"
                    only_1d_negative = False

            # compute 1d slope if present
            if "1d" in self.mtf_order:
                _, _, full_hist = self.compute_macd_for(sym, "1d", include_price=price, use_ws_current=True)
                if full_hist:
                    one_d_slope_val = slope(full_hist or [], lookback=MTF_SLOPE_LOOKBACK)

            score = 0.0
            if positive_any or any_positive_flip:
                score += 1.0
            vol_change = item.get("vol_change") or 0.0
            if vol_change and vol_change > 0:
                score += min(vol_change, 1.0)
            # add slope bonus
            if one_d_slope_val is not None and one_d_slope_val > 0:
                score += 0.5

            accept_now = False
            watch = False
            reason = ""

            # Rule C: if only 1d negative but rising, accept
            if negative_count > 0 and only_1d_negative and one_d_slope_val is not None and one_d_slope_val > 0:
                accept_now = True
                reason = "1d negative but rising"
            # Rule A: any positive across MTF or any flip
            elif positive_any or any_positive_flip:
                # if there are negative TFs as well, we prefer to treat them as watchlist (Rule B)
                if negative_count == 0:
                    accept_now = True
                    reason = "MTF positive/flip"
                else:
                    # If some TF positive but others negative, we will watch until negatives flip
                    watch = True
                    reason = "mixed polarity; watch until negatives flip"
            elif negative_count > 0:
                watch = True
                reason = "negative MTF(s); monitoring"

            evaluated.append({
                "symbol": sym,
                "root": item.get("root"),
                "price": price,
                "mtf": mtf_state,
                "positive_any": positive_any,
                "negative_count": negative_count,
                "one_d_slope": one_d_slope_val,
                "vol_change": vol_change,
                "score": score,
                "accept": accept_now,
                "watch": watch,
                "reason": reason
            })

        # act on evaluated candidates
        await self._emit_event("candidates_evaluated", evaluated)

        # If root filters removed: open according to accept/watch rules
        candidates_to_open = [e for e in evaluated if e["accept"]]
        candidates_to_watch = [e for e in evaluated if e["watch"]]

        # add to watchlist
        for w in candidates_to_watch:
            self._add_to_watchlist(w["symbol"], {"reason": w["reason"], "mtf": w["mtf"], "root": w["root"], "price": w["price"]})

        # If TRADE_ENABLED we defer openings until next cycle
        if TRADE_ENABLED:
            # Put them in deferred list
            for c in candidates_to_open:
                # store minimal info
                self._deferred_candidates.append({"symbol": c["symbol"], "price": c["price"], "score": c["score"], "reason": c["reason"]})
            logger.info("TRADE_ENABLED set: deferred %d opening(s) until next scan cycle", len(candidates_to_open))
            # in any case send notification now
            for c in candidates_to_open:
                await send_message(f"Candidate queued for open next interval: {c['symbol']} root={c['root']} score={c['score']:.2f} reason={c['reason']}")
        else:
            # open immediately (simulated or real depending on config)
            for c in candidates_to_open:
                try:
                    await self._attempt_open_from_candidate({"symbol": c["symbol"], "price": c["price"]})
                except Exception:
                    logger.exception("Error opening candidate %s", c["symbol"])

    async def _maybe_close_before_hour(self):
        """
        5 minutes before the hour: if number of open trades equals MAX_OPEN_TRADES, close the least profitable trade.
        Profit computed using last cached price, fallback to latest price query.
        """
        try:
            now = time.time()
            # time until next round hour
            next_hour = math.ceil(now / 3600.0) * 3600.0
            seconds_to_hour = next_hour - now
            if 0 < seconds_to_hour <= 300:
                logger.info("Within 5m of hour boundary (%.0fs). Checking auto-close conditions.", seconds_to_hour)
                open_trades = list(self.trade_manager.open_trades)
                if len(open_trades) >= MAX_OPEN_TRADES:
                    # compute unrealized PnL for each trade
                    best_idx = None
                    worst_pnl = None
                    for t in open_trades:
                        # get price
                        cur_price = self._last_price_cache.get(t.symbol)
                        if cur_price is None:
                            cur_price = await self.client.get_latest_price(t.symbol) or 0.0
                        try:
                            if t.side.lower() == "buy":
                                pnl = (cur_price - t.entry_price) * t.qty
                            else:
                                pnl = (t.entry_price - cur_price) * t.qty
                        except Exception:
                            pnl = -9999999.0
                        if worst_pnl is None or pnl < worst_pnl:
                            worst_pnl = pnl
                            best_idx = t
                    if best_idx:
                        # close it
                        logger.info("Auto-closing least profitable trade %s pnl=%.6f", best_idx.symbol, worst_pnl)
                        # If real trading we would place close order; fallback: mark closed
                        self.trade_manager.close_trade(best_idx, price=self._last_price_cache.get(best_idx.symbol, best_idx.entry_price))
                        await send_message(f"Auto-closed least profitable trade {best_idx.symbol} pnl={worst_pnl:.6f} (pre-hour cleanup)")
        except Exception:
            logger.exception("Error in _maybe_close_before_hour")

    async def send_summary(self, root_signals: List[Dict[str, Any]]):
        """
        Send Telegram summary with the structure you requested:
        - First block: short root tfs signal summary counts
        - Then per-root blocks (1h, 4h, 1d preferred order) listing signals with details:
            Title: Bybit Perp, symbol, price, signal strength (score), mtf alignment state
        """
        try:
            if not root_signals:
                await send_message("Root scan: no signals this interval.")
                return

            # Group by root
            grouped = {}
            for it in root_signals:
                grouped.setdefault(it["root"], []).append(it)

            # Build message
            lines = []
            # Header / summary block
            total = len(root_signals)
            lines.append(f"Root scan summary — {total} signals found")
            for rt in ROOT_TFS:
                cnt = len(grouped.get(rt, []))
                lines.append(f"- Root {rt}: {cnt} signal(s)")

            # Blank line to separate summary and detail blocks
            lines.append("\nDetailed signals:")

            # Desired ordering for detail blocks: 60 (1h), 240 (4h), 1d (D)
            detail_order = []
            for r in ROOT_TFS:
                # normalize 60/240/D etc to textual order: we'll keep existing order but ensure human-friendly label
                detail_order.append(r)

            for root in detail_order:
                lst = grouped.get(root, [])
                if not lst:
                    continue
                lines.append(f"\nRoot {root} signals:")
                # For each signal, include required fields; prefer 1h entries first for readability
                for s in lst:
                    sym = s["symbol"]
                    price = s["price"]
                    vol = s.get("vol_change")
                    vol_text = f" 24hVolΔ {vol:.2f}" if vol is not None else ""
                    # compute MTF alignment compact summary
                    mtf_state = {}
                    for tf in self.mtf_order:
                        _, _, h = self.compute_macd_for(sym, tf, include_price=price, use_ws_current=True)
                        cur = h[-1] if h and len(h) >= 1 else None
                        mtf_state[tf] = ("+" if cur is not None and cur > 0 else "-" if cur is not None and cur < 0 else "?")
                    mtf_compact = " ".join([f"{tf}:{mtf_state.get(tf,'?')}" for tf in self.mtf_order])
                    # signal strength: reuse provided score if present else compute simple score
                    score = s.get("score")
                    if score is None:
                        score = 0.0
                        if any(v == "+" for v in mtf_state.values()):
                            score += 1.0
                        if vol is not None and vol > 0:
                            score += min(vol, 1.0)
                    lines.append(f"- Bybit Perp | {sym} @ {price} | strength={score:.2f}{vol_text} | MTF={mtf_compact}")

            # Open trades snapshot
            open_sum = self.trade_manager.summary()
            if open_sum:
                lines.append("\nOpen trades:")
                for ot in open_sum:
                    lines.append(f"- {ot['symbol']} {ot['qty']} @ {ot['entry']}")

            text = "\n".join(lines)
            await send_message(text)
        except Exception:
            logger.exception("send_summary failed")

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
