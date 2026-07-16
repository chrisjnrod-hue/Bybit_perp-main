import os
import asyncio
import time
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
    MAX_CONCURRENT_REQUESTS, REST_POLL_INTERVAL, TECHNICAL_RATING, MTF_SLOPE_LOOKBACK
)
from .scanner_core import (
    normalize_klines, compute_macd_from_closes,
    detect_flip_current_open, compute_24h_volume_change_from,
    compute_tv_rating_from, compute_mtf_alignment
)

logger = get_logger("scanner_base")

SEED_KLINES_LIMIT = int(os.getenv("SEED_KLINES_LIMIT", str(KLINE_SEED_LIMIT)))
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
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        
        self._last_root_signal_send: Dict[tuple, float] = {}
        self._last_minimal_push_ts: Optional[float] = None

    def register_callback(self, cb: Callable[[str, Any], Any]):
        self._callbacks.append(cb)

    async def _emit_event(self, event: str, payload: Any):
        for cb in list(self._callbacks):
            try:
                if inspect.iscoroutinefunction(cb): await cb(event, payload)
                else: cb(event, payload)
            except Exception:
                logger.exception("Callback for event %s failed", event)

    async def _call_client_method(self, names: List[str], *args, **kwargs):
        for name in names:
            try:
                fn = getattr(self.client, name, None)
                if not fn: continue
                res = fn(*args, **kwargs)
                if inspect.isawaitable(res): res = await res
                return res
            except Exception: continue
        return None

    async def _get_symbols(self) -> List[str]:
        items = await self._call_client_method(["get_symbols", "getSymbols", "symbols"])
        if isinstance(items, dict): items = items.get("data") or items.get("result") or items
        if not isinstance(items, list): items = []
        syms = [str(it.get("symbol") or it.get("name")).upper() for it in items if it.get("symbol") or it.get("name")]
        self.symbols = sorted(set(syms))
        return self.symbols

    async def discover_symbols(self) -> List[str]:
        syms = await self._get_symbols()
        if USE_WS: await self.client.start_kline_ws()
        if not self._rest_poller_task: self._rest_poller_task = asyncio.create_task(self._rest_poller())
        return syms

    async def seed_klines_for_symbol(self, symbol: str):
        tfs = list(set(ROOT_TFS + MTF_TFS + MTF_ALIGN_TFS))
        for tf in tfs:
            try:
                async with self.request_sem:
                    raw = await self._call_client_method(["get_klines", "getKlines", "get_kline"], symbol, tf, limit=SEED_KLINES_LIMIT)
                normalized = normalize_klines(raw, tf)
                if normalized: self.kline_store[symbol][tf] = sorted(normalized, key=lambda x: x.get("start_at") or 0)
            except Exception: pass

    async def _rest_poller(self):
        while not self._stop:
            if not self.symbols:
                await asyncio.sleep(REST_POLL_INTERVAL)
                continue
            for sym in self.symbols:
                for tf in list(set(ROOT_TFS + ["5", "15"])):
                    try:
                        async with self.request_sem:
                            data = await self._call_client_method(["get_klines", "getKlines", "get_kline"], sym, tf, limit=2)
                        normalized = normalize_klines(data, tf) if data else []
                        if normalized:
                            lst = self.kline_store.get(sym, {}).setdefault(tf, [])
                            if lst and lst[-1].get("start_at") == normalized[-1].get("start_at"):
                                lst[-1] = normalized[-1]
                            else: lst.append(normalized[-1])
                    except Exception: pass
            await asyncio.sleep(REST_POLL_INTERVAL)

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        data = self.kline_store.get(symbol, {}).get(tf, [])
        closes = [float(c.get("close")) for c in data if isinstance(c, dict) and c.get("close") is not None]
        
        current_price = include_price
        if current_price is None and use_ws_current and USE_WS and hasattr(self.client, "get_ws_latest_kline"):
            ws_last = self.client.get_ws_latest_kline(symbol, tf)
            if ws_last and ws_last.get("close") is not None:
                current_price = float(ws_last.get("close"))

        # FIX: Modify last candle instead of appending to prevent 2nd candle delay
        if current_price is not None and closes:
            closes[-1] = current_price
            current_price = None 

        return compute_macd_from_closes(closes, include_price=current_price)

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0, symbol: str = "", tf: str = ""):
        return detect_flip_current_open(hist, hist_threshold)

    async def _update_24h_volume(self, symbol: str) -> Optional[float]:
        data = await self._call_client_method(["get_24h_ticker", "get24h"], symbol)
        if isinstance(data, dict):
            vol = data.get("volume") or data.get("vol")
            if vol:
                if symbol not in self._24h_volumes: self._24h_volumes[symbol] = {"current": float(vol), "previous": float(vol)}
                else:
                    self._24h_volumes[symbol]["previous"] = self._24h_volumes[symbol]["current"]
                    self._24h_volumes[symbol]["current"] = float(vol)
                return float(vol)
        return None

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        return compute_24h_volume_change_from(self._24h_volumes.get(symbol))

    def compute_tv_rating(self, symbol: str, tf: str, price: Optional[float] = None):
        klines = self.kline_store.get(symbol, {}).get(tf, [])
        return compute_tv_rating_from(klines, TECHNICAL_RATING, tf=tf, price=price)

    def _compute_mtf_alignment(self, symbol: str, price: float):
        def _get_closes(tf: str):
            return [float(c.get("close")) for c in self.kline_store.get(symbol, {}).get(tf, [])]
        return compute_mtf_alignment(_get_closes, price, MTF_ALIGN_TFS, mtf_slope_lookback=MTF_SLOPE_LOOKBACK)
