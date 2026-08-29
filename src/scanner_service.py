# scanner_service.py
"""
Scanner: core scanning, signal evaluation and trade orchestration.

This module provides a Scanner class which:
 - discovers symbols via the client,
 - seeds and keeps a kline cache (REST seeding, WS subscription if available),
 - polls/rest-fallbacks to keep latest candles,
 - computes MACD/flip signals using scanner_core helpers,
 - evaluates signals against TV rating, volume, MTF alignment and other filters,
 - delegates trade opens to TradeManager,
 - emits events (via register_callback/_emit_event) for external observers and
 - drives Telegram summaries via TelegramSummary.

The implementation aims to preserve the original operation flow and
configuration while improving readability and robustness.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import time
import inspect
from collections import defaultdict
from decimal import getcontext
from typing import Any, Callable, Dict, List, Optional, Tuple

from .logger import get_logger
from .bybit_client import BybitClient
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket
from .macd import slope  # still used by compute_mtf_alignment
from .config import (
    EXCLUDE_STABLECOINS,
    CONCURRENCY,
    KLINE_SEED_LIMIT,
    ROOT_TFS,
    MTF_TFS,
    ROOT_SCAN_INTERVAL,
    TRADE_ENABLED,
    MTF_SLOPE_LOOKBACK,
    ROOT_FILTER,
    ROOT_TOP_N,
    MAX_OPEN_TRADES,
    USE_WS,
    MAX_CONCURRENT_REQUESTS,
    REQUEST_BATCH_SIZE,
    REQUEST_BATCH_DELAY,
    REST_POLL_INTERVAL,
    VOLUME_FILTER_ENABLED,
    VOLUME_MIN_CHANGE_PCT,
    TECHNICAL_RATING,
    FLIP_CANDLE_AGE_MAX_SEC,
    SIGNAL_DEDUP_WINDOW,
    TRADE_RATING_MIN,
    TRADE_RATING_PRIORITIZE,
)

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

getcontext().prec = 28
logger = get_logger("scanner")

# local runtime overrides from env
SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")

try:
    TRADE_RATING_MIN_VAL = float(os.getenv("TRADE_RATING_MIN", str(TRADE_RATING_MIN)))
except (TypeError, ValueError):
    TRADE_RATING_MIN_VAL = TRADE_RATING_MIN

try:
    TV_RATING_WEIGHT = float(os.getenv("TV_RATING_WEIGHT", "0.3"))
    TV_RATING_WEIGHT = max(0.0, min(1.0, TV_RATING_WEIGHT))
except Exception:
    TV_RATING_WEIGHT = 0.3

TRADE_NO_NEG_VOL = os.getenv("TRADE_NO_NEG_VOL", "1").strip().lower() in ("1", "true", "yes", "y")
MARKET_CAP_MIN = float(os.getenv("MARKET_CAP_MIN", "0") or 0)
PRIORITIZE_SLOT_ORDER = [p.strip() for p in os.getenv("PRIORITIZE_SLOT_ORDER", "240,D,60").split(",") if p.strip()]

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]
TELEGRAM_DISPATCH_WINDOW = 5  # seconds window grouping TF opens for telegrams


class Scanner:
    """Main scanner class coordinating discovery, seeding, scanning, evaluation and trade execution."""

    def __init__(self) -> None:
        # rate limiter and client
        self.rate_limiter = TokenBucket(max(1.0, float(1)))
        self.client = BybitClient(rate_limiter=self.rate_limiter)

        # trade manager config (keeps existing env choices)
        tm_config = {
            "STATE_FILE": os.getenv("TRADE_STATE_FILE", "open_trades.json"),
            "MAX_OPEN_TRADES": MAX_OPEN_TRADES,
            "MIN_MARKET_CAP": MARKET_CAP_MIN or 0,
            "TP_PERCENT": float(os.getenv("TP_PERCENT", "2.0")),
            "SL_PERCENT": float(os.getenv("SL_PERCENT", "1.0")),
            "BREAKEVEN_TRIGGER_PERCENT": float(os.getenv("BREAKEVEN_TRIGGER_PERCENT", "0.5")),
            "BREAKEVEN_HIGHER_LOWS": os.getenv("BREAKEVEN_HIGHER_LOWS", "1") in ("1", "true", "True"),
            "LEVERAGE": int(os.getenv("LEVERAGE", "10")),
            "MAX_SPREAD_PERCENT": float(os.getenv("MAX_SPREAD_PERCENT", "0.1")),
            "MAX_SLIPPAGE": float(os.getenv("MAX_SLIPPAGE", "0.2")),
            "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
            "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),
            # SIMULATED if trading disabled or credentials missing
            "SIMULATED": not bool(TRADE_ENABLED) or not bool(getattr(self.client, "api_key", None)) or not bool(getattr(self.client, "api_secret", None)),
        }
        self.trade_manager = TradeManager(self.client, tm_config)

        # concurrency primitives
        self.concurrent_sem = asyncio.Semaphore(max(1, CONCURRENCY))
        self.request_sem = asyncio.Semaphore(max(1, MAX_CONCURRENT_REQUESTS))

        # kline cache [symbol][tf] -> list of candle dicts (sorted oldest->newest)
        self.kline_store: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(dict)
        self.symbols: List[str] = []

        # runtime flags & state
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._rest_poller_task: Optional[asyncio.Task] = None
        self._callbacks: List[Callable[[str, Any], Any]] = []
        self._24h_volumes: Dict[str, Dict[str, float]] = {}
        self._last_price_cache: Dict[str, float] = {}
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        self._signal_cache: Dict[Tuple[str, str, int], float] = {}

        # candle open tracking (to do full re-seed on root TF open)
        self._last_tf_candle_open_times: Dict[str, float] = {tf: 0.0 for tf in ROOT_TFS}

        # telegram manager
        self.telegram = TelegramSummary()

        logger.info(
            "scanner initialized (USE_WS=%s SEED_KLINES_LIMIT=%d CONCURRENCY=%d DEBUG_SURGICAL=%s DIAGNOSTIC=%s) "
            "TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_RATING_PRIORITIZE=%s FLIP_CANDLE_AGE_MAX_SEC=%d SIGNAL_DEDUP_WINDOW=%d",
            bool(USE_WS), SEED_KLINES_LIMIT, CONCURRENCY, DEBUG_SURGICAL_LOGS, DIAGNOSTIC_MODE,
            TRADE_RATING_MIN_VAL, TV_RATING_WEIGHT, TRADE_RATING_PRIORITIZE, FLIP_CANDLE_AGE_MAX_SEC, SIGNAL_DEDUP_WINDOW,
        )

    # ---------- utility / event wiring ----------
    def register_callback(self, cb: Callable[[str, Any], Any]) -> None:
        """Register an external callback for events (non-blocking)."""
        if not callable(cb):
            raise TypeError("callback must be callable")
        self._callbacks.append(cb)

    async def _emit_event(self, event: str, payload: Any) -> None:
        """Invoke all registered callbacks with the event payload."""
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

    async def _call_client_method(self, names: List[str], *args, **kwargs) -> Any:
        """
        Try a list of method names on the client (for robustness across different clients).
        Returns the first successful result or None.
        """
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

    # ---------- discovery & subscription ----------
    async def _get_symbols(self) -> List[str]:
        """Fetch symbols via the client and normalize to e.g. 'BTCUSDT' strings."""
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

        # unwrap common wrappers
        if isinstance(items, dict):
            if "data" in items and isinstance(items["data"], (list, dict)):
                items = items["data"]
            elif "result" in items and isinstance(items["result"], (list, dict)):
                items = items["result"]

        if isinstance(items, str):
            items = [items]

        syms: List[str] = []
        for it in items:
            try:
                if isinstance(it, str):
                    sym = it.strip().upper()
                    syms.append(sym)
                    continue
                if not isinstance(it, dict):
                    try:
                        syms.append(str(it).upper())
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

                # skip expiries / non-perps
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

                # ensure USDT perp
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
        """Public wrapper to discover symbols and (optionally) start WS subscriptions."""
        logger.info("[DIAGNOSTIC] discover_symbols: START")
        syms = await self._get_symbols()
        if not syms:
            logger.warning("[DIAGNOSTIC] discover_symbols: NO SYMBOLS FOUND!")
            return []

        # start WS if enabled
        if USE_WS:
            try:
                await self.client.start_kline_ws()
            except Exception:
                logger.exception("Failed to start client WS")

        # when WS is enabled, also subscribe to base TFs (best-effort)
        if USE_WS and syms:
            tfs_to_sub = list(set(list(ROOT_TFS) + ["5", "15"]))
            tasks: List[asyncio.Task] = []
            sem = asyncio.Semaphore(max(1, CONCURRENCY))
            for sym in syms:
                for tf in tfs_to_sub:
                    async def worker(s=sym, t=tf):
                        async with sem:
                            try:
                                if hasattr(self.client, "sub_kline"):
                                    await self.client.sub_kline(s, t)
                            except Exception:
                                logger.exception("sub_kline error for %s %s", s, t)
                    tasks.append(asyncio.create_task(worker()))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info("[DIAGNOSTIC] WS subscriptions queued for %d subscriptions", len(tasks))

        await self._ensure_rest_poller()
        logger.info("[DIAGNOSTIC] discover_symbols: COMPLETE")
        return syms

    # ---------- kline seeding & maintenance ----------
    async def _call_get_klines(self, symbol: str, tf: str, limit: int) -> Any:
        """Wrapper that tries common client method names for klines."""
        names = ["get_klines", "getKlines", "get_klines_v2", "get_kline", "getKline"]
        return await self._call_client_method(names, symbol, tf, limit)

    async def seed_klines_for_symbol(self, symbol: str) -> None:
        """Seed klines for all required TFs for a symbol."""
        if SEED_KLINES_LIMIT < 26:
            logger.warning("SEED_KLINES_LIMIT is very low (%d); MACD requires >=26 for stability", SEED_KLINES_LIMIT)

        tfs = list(set(ROOT_TFS + MTF_TFS + MTF_ALIGN_TFS))
        for tf in tfs:
            try:
                logger.debug("seed_klines_for_symbol: requesting %s %s limit=%d", symbol, tf, SEED_KLINES_LIMIT)
                async with self.request_sem:
                    raw = await self._call_get_klines(symbol, tf, limit=SEED_KLINES_LIMIT)

                if not raw:
                    logger.debug("No klines returned for %s %s (raw empty)", symbol, tf)
                    continue

                if DEBUG_SURGICAL_LOGS:
                    try:
                        sample_raw = None
                        if isinstance(raw, dict):
                            for key in ("list", "result", "data"):
                                if key in raw and isinstance(raw[key], (list, tuple)):
                                    sample_raw = raw[key][:3]
                                    break
                        elif isinstance(raw, (list, tuple)):
                            sample_raw = raw[:3]
                        logger.info("[SURGICAL_LOG] %s %s - sample=%s", symbol, tf, sample_raw)
                    except Exception:
                        logger.debug("Failed surgical sample logging", exc_info=True)

                normalized = normalize_klines(raw, tf)

                valid: List[Dict[str, Any]] = []
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
                        snippet = json.dumps(raw, default=str)[:500]
                    except Exception:
                        snippet = str(raw)[:500]
                    logger.debug("Seeded 0 usable candles for %s %s. Raw (truncated): %s", symbol, tf, snippet)
                    continue

                try:
                    klines_sorted = sorted(valid, key=lambda x: x.get("start_at") or 0)
                except Exception:
                    klines_sorted = valid

                self.kline_store[symbol][tf] = klines_sorted
                logger.warning("[SEED_COMPLETE] %s %s: seeded %d candles", symbol, tf, len(klines_sorted))
                await self._emit_event("klines_seeded", {"symbol": symbol, "tf": tf, "count": len(klines_sorted)})
            except Exception:
                logger.exception("Seed klines failed for %s %s", symbol, tf)

    async def seed_all(self) -> None:
        """Seed klines for all known symbols (batched)."""
        logger.info("[DIAGNOSTIC] seed_all: START (symbols=%d)", len(self.symbols))

        async def worker(sym: str) -> None:
            async with self.concurrent_sem:
                await self.seed_klines_for_symbol(sym)

        for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
            batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
            tasks = [asyncio.create_task(worker(s)) for s in batch]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if i + REQUEST_BATCH_SIZE < len(self.symbols):
                await asyncio.sleep(REQUEST_BATCH_DELAY)

        logger.info("[DIAGNOSTIC] seed_all: COMPLETE")

    # ---------- REST poller fallback (when WS unavailable) ----------
    async def _rest_poller(self) -> None:
        """Poll small number of candles periodically to keep kline_store fresh."""
        logger.info("REST poller started (interval=%s)", REST_POLL_INTERVAL)
        poll_count = 0
        try:
            while not self._stop and (not USE_WS or not self.client.is_ws_connected()):
                poll_count += 1
                if poll_count % 5 == 0:
                    logger.info("[REST_POLLER] poll #%d symbols=%d", poll_count, len(self.symbols))
                start_ts = time.time()

                if not self.symbols:
                    await asyncio.sleep(REST_POLL_INTERVAL)
                    continue

                async def poll_symbol(sym: str) -> None:
                    tfs_to_poll = list(set(ROOT_TFS + ["5", "15"]))
                    for tf in tfs_to_poll:
                        try:
                            async with self.request_sem:
                                data = await self._call_get_klines(sym, tf, limit=3)
                                normalized = normalize_klines(data, tf) if data else []
                                if not normalized:
                                    continue

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
                                    lst = self.kline_store.get(sym, {}).get(tf)
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

                # run in batches
                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(poll_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks, return_exceptions=True)

                elapsed = time.time() - start_ts
                to_sleep = max(0, REST_POLL_INTERVAL - elapsed)

                # stop poller if WS reconnected
                if USE_WS and self.client.is_ws_connected():
                    logger.info("WS reconnected; stopping REST poller")
                    break

                await asyncio.sleep(to_sleep)
        except asyncio.CancelledError:
            logger.info("REST poller cancelled")
        except Exception:
            logger.exception("REST poller encountered exception")
        logger.info("REST poller stopped")

    async def _ensure_rest_poller(self) -> None:
        """Start REST poller task if WS not connected."""
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

        logger.info("[REST_POLLER_START] WS unavailable, starting REST poller")
        self._rest_poller_task = asyncio.create_task(self._rest_poller())

    # ---------- small wrappers using scanner_core ----------
    def _tf_to_seconds(self, tf: str) -> int:
        return tf_to_seconds(tf)

    def _normalize_klines(self, raw_klines: Any, tf: str) -> List[Dict[str, Any]]:
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
        Build closes list from kline_store and call scanner_core.compute_macd_from_closes.
        If include_price is None and use_ws_current is True, try client's WS latest kline.
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

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = "") -> bool:
        """Wrapper for detect_flip_current_open from scanner_core (keeps same signature used elsewhere)."""
        return detect_flip_current_open(hist, hist_threshold)

    # ---------- volume tracking ----------
    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        """Fetch and update stored 24h volume for symbol (stores both previous and current)."""
        try:
            names = ["get_24h_ticker", "get24h", "get_24h", "get_ticker_24h", "ticker_24h", "get_ticker"]
            data = await self._call_client_method(names, symbol)
            if not data:
                logger.debug("[VOLUME_UPDATE] No ticker data for %s", symbol)
                return None

            # unwrap expected wrappers
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], dict):
                    data = data["data"]
                elif "result" in data and isinstance(data["result"], dict):
                    data = data["result"]

            vol = None
            if isinstance(data, dict):
                for key in ("volume", "vol", "turnover", "volume24h", "quote_vol", "volume_24h", "volume24"):
                    if key in data and data.get(key) is not None:
                        try:
                            vol = float(data.get(key))
                            break
                        except Exception:
                            try:
                                vol = float(str(data.get(key)).replace(",", ""))
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
                logger.debug("[VOLUME_UPDATE] %s current=%.0f", symbol, vol)
                return vol
            else:
                logger.debug("[VOLUME_UPDATE] No volume field matched for %s (raw=%s)", symbol, str(data)[:200])
        except Exception:
            logger.debug("Could not update 24h volume for %s", symbol, exc_info=True)
        return None

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        return compute_24h_volume_change_from(self._24h_volumes.get(symbol))

    # ---------- TV rating & MTF alignment wrappers ----------
    def compute_tv_rating(self, symbol: str, tf: str, price: Optional[float] = None) -> Tuple[float, str]:
        klines = self.kline_store.get(symbol, {}).get(tf, [])
        return compute_tv_rating_from(klines, TECHNICAL_RATING, tf=tf, price=price)

    def _compute_mtf_alignment(self, symbol: str, price: float) -> Dict[str, Any]:
        def _get_closes(tf: str) -> List[float]:
            items = self.kline_store.get(symbol, {}).get(tf, [])
            closes: List[float] = []
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

    # ---------- deduplication for signals ----------
    def _try_dedupe_signal(self, symbol: str, tf: str, candle_open_time: Optional[int], now_ts: float) -> bool:
        """
        Returns True when the signal is considered NEW (and caches it).
        Returns False if duplicate within SIGNAL_DEDUP_WINDOW.
        """
        if SIGNAL_DEDUP_WINDOW <= 0:
            return True
        if candle_open_time is None:
            logger.debug("Cannot dedupe signal: candle_open_time is None")
            return True
        try:
            cache_key = (symbol, tf, int(candle_open_time))
            last = self._signal_cache.get(cache_key)
            if last is not None:
                if now_ts - last < SIGNAL_DEDUP_WINDOW:
                    logger.debug("DEDUPE BLOCKED %s %s open=%d (last %.0f sec ago)", symbol, tf, candle_open_time, now_ts - last)
                    return False
            self._signal_cache[cache_key] = now_ts
            logger.debug("DEDUPE PASSED %s %s open=%d", symbol, tf, candle_open_time)
            return True
        except Exception as e:
            logger.debug("Error in dedupe check: %s", e)
            return True

    # ---------- core scan loop ----------
    async def root_scan_loop(self) -> None:
        """Main loop scanning root TFs and processing signals."""
        logger.info("[DIAGNOSTIC] root_scan_loop: START interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1
            cycle_start = time.time()
            logger.info("[DIAGNOSTIC] root_scan_loop: cycle #%d start", loop_count)

            try:
                # ensure symbols seeded
                if not self.symbols:
                    logger.info("[DIAGNOSTIC] No symbols - discovering")
                    await self.discover_symbols()
                    if self.symbols:
                        logger.info("[DIAGNOSTIC] Seeding klines for %d symbols", len(self.symbols))
                        await self.seed_all()
                        logger.info("[DIAGNOSTIC] Seeding complete")
                    else:
                        logger.warning("[DIAGNOSTIC] No symbols discovered - sleeping")
                        await asyncio.sleep(10)
                        continue

                await self._ensure_rest_poller()

                # detect root TF candle opens and seed if any opened
                now_ts = time.time()
                refreshed_roots: List[str] = []
                for root in ROOT_TFS:
                    current_start = self._get_current_candle_start(root, now_ts)
                    last_start = self._last_tf_candle_open_times.get(root, 0.0)
                    if current_start > last_start:
                        logger.info("[CANDLE_OPEN] root=%s opened=%d (was %d)", root, int(current_start), int(last_start))
                        self._last_tf_candle_open_times[root] = float(current_start)
                        refreshed_roots.append(root)

                if refreshed_roots:
                    logger.info("[CANDLE_OPEN_REFRESH] New root TF opens: %s - reseeding all candles", refreshed_roots)
                    await self.seed_all()
                    logger.info("[CANDLE_OPEN_REFRESH] reseed complete")

                root_signals: List[Dict[str, Any]] = []
                logger.info("[DIAGNOSTIC] Checking %d symbols", len(self.symbols))

                async def check_symbol(sym: str) -> None:
                    try:
                        # get price (REST primary, WS fallback)
                        async with self.request_sem:
                            price = await self.client.get_latest_price(sym)
                        if price is None and USE_WS and hasattr(self.client, "get_ws_latest_kline"):
                            try:
                                ws_last = self.client.get_ws_latest_kline(sym, ROOT_TFS[0]) if ROOT_TFS else None
                                if ws_last and ws_last.get("close") is not None:
                                    price = float(ws_last.get("close"))
                            except Exception:
                                price = None

                        if price is None:
                            return

                        self._last_price_cache[sym] = price

                        # ensure volume updated before evaluating signals
                        await self._update_24h_volume(sym)

                        now_check = time.time()
                        for root in ROOT_TFS:
                            logger.info("[ROOT_SCAN_CALC] %s %s computing MACD", sym, root)
                            macd_line, sig_line, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)

                            # normalize hist to list of floats
                            hist_list: List[float] = []
                            try:
                                if hist is None:
                                    hist_list = []
                                elif isinstance(hist, (list, tuple)):
                                    hist_list = [float(x) for x in hist]
                                else:
                                    try:
                                        hist_list = list(map(float, hist))
                                    except Exception:
                                        hist_list = [float(hist)] if isinstance(hist, (int, float)) else []
                            except Exception:
                                hist_list = []

                            logger.info("[SURGICAL_MACD_SNAPSHOT] %s %s macd=%s signal=%s hist_len=%d last2=%s",
                                        sym, root,
                                        ("%.6f" % macd_line) if isinstance(macd_line, (int, float)) else str(macd_line),
                                        ("%.6f" % sig_line) if isinstance(sig_line, (int, float)) else str(sig_line),
                                        len(hist_list),
                                        str(hist_list[-2:]) if hist_list else "[]")

                            # primary flip detection
                            flip = False
                            try:
                                flip = bool(self.detect_flip_current_open(hist_list, 0.0, symbol=sym, tf=root))
                            except Exception:
                                flip = False

                            # conservative heuristic: prev < 0 and last > 0
                            if not flip and len(hist_list) >= 2:
                                try:
                                    prev_h = float(hist_list[-2])
                                    last_h = float(hist_list[-1])
                                    if prev_h < 0 and last_h > 0:
                                        flip = True
                                    elif prev_h <= 0 and last_h > 0 and abs(last_h) > 1e-6:
                                        flip = True
                                except Exception:
                                    flip = False

                            logger.info("[ROOT_SCAN_RESULT] %s %s flip=%s hist_len=%d", sym, root, flip, len(hist_list))

                            if hist_list and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                start_at = None
                                try:
                                    last_rows = self.kline_store.get(sym, {}).get(root, [])
                                    if last_rows:
                                        start_at = last_rows[-1].get("start_at")
                                except Exception:
                                    start_at = None

                                candle_age_ok = self._is_candle_age_acceptable(start_at, now_check)
                                is_new_signal = self._try_dedupe_signal(sym, root, start_at, now_check)
                                if not is_new_signal:
                                    logger.info("SIGNAL REJECTED (duplicate): %s %s @ %s", sym, root, price)
                                    continue

                                tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                                mtf_align = self._compute_mtf_alignment(sym, price)
                                sig_item = {
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist_list,
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
                                    logger.info("SIGNAL DETECTED (OLD_CANDLE): %s %s @ %s (tv=%s %.3f age=%.0f)", sym, root, price, tv_label, tv_score, now_check - (start_at or now_check))
                                else:
                                    logger.info("SIGNAL DETECTED: %s %s @ %s (tv=%s %.3f age=%.0f)", sym, root, price, tv_label, tv_score, now_check - (start_at or now_check))

                    except Exception:
                        logger.exception("Error checking symbol %s", sym)

                # iterate in batches to avoid overwhelming client
                checked_count = 0
                for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                    if self._stop:
                        break
                    batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                    tasks = [asyncio.create_task(check_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks, return_exceptions=True)
                    checked_count += len(batch)
                    if i + REQUEST_BATCH_SIZE < len(self.symbols):
                        await asyncio.sleep(REQUEST_BATCH_DELAY)

                logger.info("[DIAGNOSTIC] scan finished checked=%d signals_found=%d", checked_count, len(root_signals))

                # handle monitored (waiting) symbols that might have just aligned
                newly_aligned = await self._check_monitored_symbols()
                evaluated_aligned = []
                if newly_aligned:
                    evaluated_aligned = await self.handle_root_signals(newly_aligned, allow_open_trades=True)

                # subscribe to MTF for signals on full push cycles
                now_ts2 = time.time()
                is_full_push = self.telegram.check_full_push(now_ts2)
                if root_signals and is_full_push:
                    for sig in root_signals:
                        try:
                            sym = sig["symbol"]
                            if USE_WS and hasattr(self.client, "subscribe_mtf_for_symbol"):
                                await self.client.subscribe_mtf_for_symbol(sym, MTF_TFS)
                        except Exception:
                            logger.exception("Failed to request MTF subscribe for %s", sig.get("symbol"))

                # evaluate and possibly open trades
                evaluated_signals = []
                if root_signals:
                    evaluated_signals = await self.handle_root_signals(root_signals, allow_open_trades=is_full_push)

                # merge for telegram
                if newly_aligned:
                    root_signals.extend(newly_aligned)
                    evaluated_signals.extend(evaluated_aligned)

                # dispatch telegram summary
                if hasattr(self.telegram, "send_summary"):
                    try:
                        await self.telegram.send_summary(root_signals=root_signals, evaluated=evaluated_signals, full_push=is_full_push, is_candle_open=is_full_push)
                    except Exception:
                        logger.exception("Failed to dispatch Telegram summary")
                if is_full_push:
                    self.telegram.mark_full_push_sent()

            except Exception:
                logger.exception("Error in root_scan_loop (outer)")

            # sleep logic
            elapsed = time.time() - cycle_start
            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] sleeping %.1f seconds before next cycle", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                # align to next 5m candle open (original behavior)
                now_sleep = time.time()
                now_struct = time.gmtime(now_sleep)
                current_minute = now_struct.tm_min
                current_second = now_struct.tm_sec
                next_5m_minute = ((current_minute // 5) + 1) * 5
                if next_5m_minute >= 60:
                    to_sleep = (60 - current_minute) * 60 - current_second
                else:
                    to_sleep = ((next_5m_minute - current_minute) * 60) - current_second
                to_sleep = max(1, min(300, to_sleep))
                logger.info("[DIAGNOSTIC] Aligning to next 5m candle open: sleeping %.1f seconds", to_sleep)
                await asyncio.sleep(to_sleep)

    # ---------- monitored symbols (waiting for MTF alignment) ----------
    async def _check_monitored_symbols(self) -> List[Dict[str, Any]]:
        """Re-evaluate symbols currently in monitoring state (waiting for negative TF to flip)."""
        newly_aligned: List[Dict[str, Any]] = []
        if not self._mtf_monitoring:
            return newly_aligned

        MONITORING_MAX_AGE = 86400  # 24h
        now_ts = time.time()
        to_remove: List[str] = []

        for sym, info in list(self._mtf_monitoring.items()):
            try:
                if now_ts - info.get("started_at", now_ts) > MONITORING_MAX_AGE:
                    logger.info("MONITORING EXPIRED (%s): removing %s", MONITORING_MAX_AGE, sym)
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
                status = mtf_align.get("status", "monitoring")
                if status in ("aligned", "daily_rising"):
                    logger.info("MONITORING RESOLVED: %s -> %s (queuing open)", sym, status)
                    to_remove.append(sym)
                    vol_change = self.compute_24h_volume_change(sym)
                    resolved_item = {
                        "symbol": sym,
                        "root": info.get("root"),
                        "price": price,
                        "hist": [],
                        "vol_change": vol_change,
                        "from_monitoring": True,
                        "mtf_status": status,
                        "negative_tfs": mtf_align.get("negative_tfs", []),
                        "score": sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip")) + (min(vol_change, 1.0) if vol_change is not None and vol_change > 0 else 0.0),
                    }
                    newly_aligned.append(resolved_item)
                else:
                    prev_neg = set(info.get("negative_tfs", []))
                    curr_neg = set(mtf_align.get("negative_tfs", []))
                    if curr_neg != prev_neg:
                        self._mtf_monitoring[sym]["negative_tfs"] = list(curr_neg)
                        self._mtf_monitoring[sym]["last_alert"] = now_ts
            except Exception:
                logger.exception("Error re-checking monitored symbol %s", sym)

        for sym in to_remove:
            self._mtf_monitoring.pop(sym, None)

        return newly_aligned

    # ---------- evaluate signals and (optionally) open trades ----------
    async def handle_root_signals(self, root_signals: List[Dict[str, Any]], allow_open_trades: bool = True) -> List[Dict[str, Any]]:
        """
        Evaluate candidate root signals and, if allowed, attempt to open trades via TradeManager.
        Returns evaluated entries (with accept/reason and additional metadata).
        """
        evaluated: List[Dict[str, Any]] = []
        to_open: List[Dict[str, Any]] = []

        # build evaluation entries
        for item in root_signals:
            sym = item.get("symbol")
            price = item.get("price")
            root = item.get("root")
            vol_change = item.get("vol_change")
            tv_label = item.get("tv_label")
            tv_score = item.get("tv_score", 0.0)
            start_at = item.get("start_at")

            candle_age_ok = item.get("candle_age_ok")
            if candle_age_ok is None:
                candle_age_ok = self._is_candle_age_acceptable(start_at, time.time())

            hist = item.get("hist") or []
            if not hist:
                _, _, hist = self.compute_macd_for(sym, root, include_price=price)
                hist = hist or []
            macd_hist_val = hist[-1] if hist else 0.0

            mtf_align = self._compute_mtf_alignment(sym, price)
            mtf_status = mtf_align.get("status", "monitoring")
            negative_tfs = mtf_align.get("negative_tfs", [])

            score = sum(1.0 for d in mtf_align["tfs"].values() if d.get("is_positive")) + sum(0.5 for d in mtf_align["tfs"].values() if d.get("is_flip"))
            if vol_change is not None and vol_change > 0:
                score += min(vol_change, 1.0)

            entry: Dict[str, Any] = {
                "symbol": sym,
                "root": root,
                "price": price,
                "hist": hist,
                "macd_hist_val": macd_hist_val,
                "mtf": mtf_align.get("tfs", {}),
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

            # Evaluate acceptance rules
            if mtf_status in ("aligned", "daily_rising"):
                # candle age
                if not candle_age_ok:
                    entry["accept"] = False
                    entry["reason"] = "candle_too_old"
                    logger.info("Trade blocked (candle too old): %s root=%s", sym, root)
                    evaluated.append(entry)
                    continue

                # TV rating gate
                if TRADE_RATING_MIN_VAL > 0.0 and tv_score < TRADE_RATING_MIN_VAL:
                    entry["accept"] = False
                    entry["reason"] = f"tv_rating_below_threshold_{tv_score:.4f}"
                    logger.info("Trade blocked by TV rating: %s tv_score=%.4f min=%.4f", sym, tv_score, TRADE_RATING_MIN_VAL)
                    evaluated.append(entry)
                    continue

                # market cap gate (best-effort)
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
                                            marketcap = float(str(symbol_info.get(key)).replace(",", ""))
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

                # volume filters
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

        # prioritize by TV rating (if requested)
        if TRADE_RATING_PRIORITIZE and candidates:
            candidates = sorted(candidates, key=lambda c: c.get("tv_score", 0.0), reverse=True)
            logger.info("Sorted %d candidates by TV rating", len(candidates))

        # apply root filtering / slot allocation
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
                selected.extend(remaining_candidates[:remaining_slots])

            candidates = sorted(selected, key=lambda r: self._compute_combined_score(r), reverse=True)
        else:
            # honor PRIORITIZE_SLOT_ORDER as a soft slot ordering if set
            if PRIORITIZE_SLOT_ORDER:
                grouped: Dict[str, List[Dict[str, Any]]] = {}
                remaining_slots = max(0, MAX_OPEN_TRADES - (len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0))
                selected: List[Dict[str, Any]] = []
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
                    selected.extend(remaining_candidates[:remaining_slots])
                candidates = sorted(selected, key=lambda r: self._compute_combined_score(r), reverse=True)
            else:
                candidates = sorted(candidates, key=lambda r: self._compute_combined_score(r), reverse=True)

        eval_map: Dict[Tuple[str, str], Dict[str, Any]] = {(e["symbol"], e["root"]): e for e in evaluated}

        # If open trades suppressed (e.g., telepush-only cycle), mark and return
        if not allow_open_trades:
            for c in candidates:
                c["open_suppressed"] = True
                ev = eval_map.get((c["symbol"], c["root"]))
                if ev is not None:
                    ev["open_suppressed"] = True
                    ev["accept"] = False
                    ev["reason"] = "open_suppressed"
            return evaluated

        # attempt opens
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
                ev = eval_map.get((c["symbol"], c["root"]))
                if ev is not None:
                    ev["accept"] = False
                    ev["reason"] = "negvol_blocked"
                continue

            # get balance for allocation
            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None

            try:
                tm_result = await self.trade_manager.open_trade(sym, "BUY", price, balance)
            except Exception as e:
                logger.exception("TradeManager.open_trade raised for %s: %s", sym, e)
                tm_result = {"success": False, "error": str(e)}

            ev = eval_map.get((sym, c["root"]))
            if tm_result.get("success"):
                if ev is not None:
                    ev["accept"] = True
                    ev["reason"] = "opened" if not tm_result.get("simulated") else "simulated"
                    if "order" in tm_result:
                        ev["order"] = tm_result.get("order")
                    ev["trade_record"] = tm_result.get("trade_record")
            else:
                err = tm_result.get("error", "open_failed")
                c["accept"] = False
                c["reason"] = err
                if ev is not None:
                    ev["accept"] = False
                    ev["reason"] = err

            # if we attempt to open trade for a monitored symbol, remove it from monitoring
            self._mtf_monitoring.pop(sym, None)

        return evaluated

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        """Combine MTF score and TV rating with environmental weight."""
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - TV_RATING_WEIGHT)) + (tv_score * TV_RATING_WEIGHT)
        return combined

    # ---------- run / stop ----------
    async def run(self) -> None:
        """Start scanner root loop as a background task and await it."""
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

    def stop(self) -> None:
        """Stop the scanner loop and cancel background tasks."""
        logger.info("Stopping scanner...")
        self._stop = True
        if self._task and not self._task.done():
            self._task.cancel()
        if self._rest_poller_task and not self._rest_poller_task.done():
            try:
                self._rest_poller_task.cancel()
            except Exception:
                pass
