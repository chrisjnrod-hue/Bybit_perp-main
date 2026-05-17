"""
src/scanner.py - Robust scanner adapted from your JS logic.

Features:
- Robust symbol discovery (handles v5 and v2 shapes)
- Closed-candle MACD seeding (drops live in-progress candle)
- Handles both array-shaped and object-shaped klines
- Keeps prevHist == 0 as valid
- Minimal event callback system (emit/on)
- Defensive: logs and returns empty lists when remote responses are unexpected
"""

import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Callable, Iterable

from .logger import get_logger
from .config import STABLE_COINS, ROOT_TFS, MTF_TFS, SEED_HISTORICAL, HIST_LOOKBACK
from .storage import read_signals, write_signals
from .macd import MACD  # your MACD class - must implement update()/get()/peek()
from .ratelimiter import TokenBucket  # if used by your rest client

logger = get_logger("scanner")


def normalize_interval_to_tf(interval: Optional[str]) -> Optional[str]:
    if not interval:
        return interval
    s = str(interval).strip().lower()
    if s.isdigit():
        return s
    if s.endswith('m') and s[:-1].isdigit():
        return str(int(s[:-1]))
    if s.endswith('h') and s[:-1].isdigit():
        return str(int(s[:-1]) * 60)
    if s in ('d', '1d', 'day'):
        return 'D'
    # fallback
    return interval


def tf_to_seconds(tf: str) -> int:
    if str(tf).upper() == 'D':
        return 24 * 3600
    return int(tf) * 60


class Scanner:
    def __init__(self, rest_client: Any = None, ws_client: Any = None, trader: Any = None):
        """
        rest_client: instance that exposes symbol and kline endpoints (the scanner will probe for method names)
        ws_client: optional, may implement subscribe(topic) and on_message callbacks; if not provided the scanner will rely on REST only
        trader: optional trading client used by mtf evaluation when opening trades
        """
        self.rest = rest_client
        self.ws = ws_client
        self.trader = trader

        # internal state
        self.symbols = set()  # set of symbol strings
        self.symbolData: Dict[str, Dict[str, Any]] = {}  # lastClose/lastOpen/lastCandleStart etc
        self.activeRootSignals: Dict[str, Dict[str, Any]] = {}
        self.rootIndex: Dict[str, str] = {}
        self.pendingSignals: List[Dict[str, Any]] = []
        self._persist_task = None

        # per-run cache: passCache[symbol][tf] = {...}
        self.passCache: Dict[str, Dict[str, Any]] = {}

        # concurrency controls
        self._rest_sem = asyncio.Semaphore(int(getattr(self, "REST_BATCH_SIZE", 6)))
        self._scan_sem = asyncio.Semaphore(int(getattr(self, "SCAN_CONCURRENCY", 6)))

        # event callbacks: event -> list of callables
        self._events: Dict[str, List[Callable[..., Any]]] = {}

        # hook WS message if ws client provided
        if self.ws:
            try:
                # prefer ws.on interface if provided; otherwise fallback to attribute
                on_fn = getattr(self.ws, "on", None)
                if callable(on_fn):
                    on_fn("message", self._handle_ws_message)
                else:
                    # if ws uses a different handler, user must wire it manually
                    logger.debug("ws client provided but no 'on' method found; please wire messages to scanner.handle_ws_message")
            except Exception:
                logger.exception("ws wiring failure")

        # load persisted signals
        self._load_persisted_signals()

    # -------------------------
    # Event helpers
    # -------------------------
    def on(self, event: str, cb: Callable[..., Any]):
        self._events.setdefault(event, []).append(cb)

    def emit(self, event: str, *args, **kwargs):
        handlers = list(self._events.get(event, []))
        for h in handlers:
            try:
                if asyncio.iscoroutinefunction(h):
                    asyncio.create_task(h(*args, **kwargs))
                else:
                    h(*args, **kwargs)
            except Exception:
                logger.exception("Event handler error for %s", event)

    # -------------------------
    # Persistence
    # -------------------------
    def _load_persisted_signals(self):
        try:
            arr = read_signals() or []
            roots = [x for x in arr if x and x.get("type") == "root" and x.get("status") == "active"]
            for r in roots:
                key = f"{r.get('symbol')}|{r.get('tf')}|{r.get('start')}"
                self.activeRootSignals[key] = r
                if r.get("id"):
                    self.rootIndex[r["id"]] = key
            logger.info("Loaded persisted root signals: %d", len(self.activeRootSignals))
            self.emit("signals_loaded", {"count": len(self.activeRootSignals)})
        except Exception:
            logger.exception("Failed to load persisted signals")

    def _ensure_persist_loop(self, interval_ms: int = 1000):
        if self._persist_task and not self._persist_task.done():
            return
        async def _persist_loop():
            while True:
                try:
                    if self.pendingSignals:
                        to_write = self.pendingSignals[:]
                        self.pendingSignals.clear()
                        cur = read_signals() or []
                        cur.extend(to_write)
                        write_signals(cur)
                        logger.info("Persisted %d pending signals", len(to_write))
                    await asyncio.sleep(max(0.5, interval_ms / 1000.0))
                except Exception:
                    logger.exception("persist loop error")
                    await asyncio.sleep(max(0.5, interval_ms / 1000.0))
        self._persist_task = asyncio.create_task(_persist_loop())

    # -------------------------
    # REST helper detection
    # -------------------------
    def _rest_method(self, *candidates):
        for name in candidates:
            if not self.rest:
                break
            if hasattr(self.rest, name):
                return getattr(self.rest, name)
        return None

    # -------------------------
    # Symbol discovery (robust)
    # -------------------------
    async def _fetch_symbols(self):
        """
        Robust symbol discovery based on the JS logic you provided.
        Pulls symbols from rest client; accepts many shapes and builds USDT perpetual list.
        """
        try:
            # find a rest method to retrieve symbols (supports get_symbols or getSymbols)
            get_symbols_fn = self._rest_method("get_symbols", "getSymbols", "getSymbolsAsync", "getSymbolsSync")
            # If rest client exposes a method with different naming, attempt 'getSymbols' style
            if get_symbols_fn is None:
                # some clients name it 'getSymbols' or 'get_symbols'
                get_symbols_fn = self._rest_method("get_symbols", "getSymbols")

            resp = None
            if get_symbols_fn:
                try:
                    # support async or sync
                    if asyncio.iscoroutinefunction(get_symbols_fn):
                        resp = await get_symbols_fn()
                    else:
                        resp = get_symbols_fn()
                except Exception as e:
                    logger.debug("get_symbols function raised: %s", e)
                    resp = None
            else:
                logger.warning("No rest.get_symbols / getSymbols method found on rest client")
                resp = None

            # normalize instruments list from various shapes
            instruments = []
            if resp is None:
                instruments = []
            elif isinstance(resp, list):
                instruments = resp
            elif isinstance(resp, dict):
                # common shapes: { "result": { "list": [...] } } or { "result": [...] } or { "data": [...] }
                if "result" in resp:
                    r = resp.get("result")
                    if isinstance(r, dict) and isinstance(r.get("list"), list):
                        instruments = r.get("list", [])
                    elif isinstance(r, list):
                        instruments = r
                    elif isinstance(r, dict):
                        # convert dict values
                        instruments = list(r.values())
                    else:
                        instruments = []
                elif "data" in resp:
                    d = resp.get("data")
                    if isinstance(d, list):
                        instruments = d
                    elif isinstance(d, dict):
                        instruments = list(d.values())
                    else:
                        instruments = []
                else:
                    # try to coerce dict values
                    instruments = list(resp.values())
            else:
                # unknown type - ignore
                instruments = []

            # log a small sample for debugging
            try:
                logger.info("bybit_client - raw symbols sample (first 5): %s", instruments[:5])
            except Exception:
                logger.debug("could not log sample symbols")

            added = 0
            for inst in instruments:
                symbol = None
                # plain string instrument
                if isinstance(inst, str):
                    symbol = inst
                elif isinstance(inst, dict):
                    # prefer symbol-like keys
                    symbol = inst.get("symbol") or inst.get("name") or inst.get("instId") or inst.get("instrument_name") or inst.get("instrumentName") or inst.get("symbolName") or inst.get("symbol_name")
                    if not symbol:
                        base = inst.get("baseCoin") or inst.get("base") or inst.get("base_currency") or inst.get("underlying")
                        quote = inst.get("quoteCoin") or inst.get("quote") or inst.get("quote_currency")
                        if base and quote:
                            symbol = f"{base}{quote}"
                    # if instrument provides explicit 'instrumentType' verify it's PERPETUAL
                    typ = inst.get("instrumentType") or inst.get("instrument_type")
                    if typ and str(typ).upper() != "PERPETUAL":
                        continue
                    # if quote field exists and is not USDT skip
                    q = inst.get("quoteCoin") or inst.get("quote")
                    if q and str(q).upper() != "USDT":
                        continue
                else:
                    continue

                if not symbol:
                    continue
                symbol = str(symbol).upper().strip()
                if not symbol.endswith("USDT"):
                    continue
                base = symbol[:-4]
                if STABLE_COINS and base in STABLE_COINS:
                    continue
                if symbol in self.symbols:
                    continue
                self.symbols.add(symbol)
                self.symbolData.setdefault(symbol, {})
                added += 1

            logger.info("Fetched symbols via REST: added=%d total=%d", added, len(self.symbols))
            self.emit("symbols_fetched", {"count": len(self.symbols)})
        except Exception:
            logger.exception("fetchSymbols error")

    # -------------------------
    # Kline retrieval helper (defensive)
    # -------------------------
    async def _get_klines(self, symbol: str, tf: str, limit: int = HIST_LOOKBACK) -> List[Any]:
        """
        Try rest.get_klines, getKlines, or getKlines; return list or [].
        Throttle via semaphore to avoid hammering API.
        """
        fn = self._rest_method("get_klines", "getKlines", "getKlinesAsync", "getKline")
        if fn is None:
            logger.warning("No rest.get_klines / getKlines method found on rest client")
            return []
        # acquire semaphore
        async with self._rest_sem:
            try:
                if asyncio.iscoroutinefunction(fn):
                    resp = await fn(symbol, tf, limit)
                else:
                    resp = fn(symbol, tf, limit)
            except Exception as e:
                logger.debug("_get_klines call raised for %s %s: %s", symbol, tf, e)
                return []
        # normalize response shapes (common: {'result': {'list': [...]}} or {'result': [...] } or list)
        try:
            if resp is None:
                return []
            if isinstance(resp, list):
                out = resp
            elif isinstance(resp, dict):
                if "result" in resp:
                    r = resp.get("result")
                    if isinstance(r, dict) and isinstance(r.get("list"), list):
                        out = r.get("list")
                    elif isinstance(r, list):
                        out = r
                    else:
                        out = list(r.values()) if isinstance(r, dict) else []
                elif "data" in resp and isinstance(resp.get("data"), list):
                    out = resp.get("data")
                else:
                    # try to coerce to list
                    out = list(resp.values())
            else:
                out = []
            # ensure ordering oldest -> newest (if timestamps decreasing, reverse)
            if isinstance(out, list) and len(out) >= 2:
                def _start_ts(item):
                    if isinstance(item, (list, tuple)):
                        return float(item[0] if len(item) > 0 else 0)
                    if isinstance(item, dict):
                        return float(item.get("start") or item.get("t") or item.get("open_time") or item.get("ts") or 0)
                    return 0
                try:
                    a = _start_ts(out[0])
                    b = _start_ts(out[-1])
                    if a > b:
                        out = out[::-1]
                except Exception:
                    pass
            return out if isinstance(out, list) else []
        except Exception:
            logger.exception("_get_klines normalization error")
            return []

    # -------------------------
    # Ensure symbol lastOpen/lastClose
    # -------------------------
    async def _ensure_symbol_data_populated(self, symbol: str):
        if not symbol:
            return
        data = self.symbolData.get(symbol, {})
        if isinstance(data.get("lastClose"), (int, float)) and isinstance(data.get("lastOpen"), (int, float)):
            return
        try:
            candles = await self._get_klines(symbol, "5", 2)
            if candles and len(candles) >= 1:
                last = candles[-1]
                if isinstance(last, (list, tuple)):
                    open_p = float(last[1]) if len(last) > 1 else None
                    close_p = float(last[4]) if len(last) > 4 else None
                    ts = int(last[0]) if len(last) > 0 else None
                else:
                    open_p = float(last.get("open") or last.get("o") or 0)
                    close_p = float(last.get("close") or last.get("c") or 0)
                    ts = int(last.get("start") or last.get("t") or last.get("open_time") or 0)
                self.symbolData.setdefault(symbol, {})
                if open_p is not None:
                    self.symbolData[symbol]["lastOpen"] = open_p
                if close_p is not None:
                    self.symbolData[symbol]["lastClose"] = close_p
                if ts:
                    self.symbolData[symbol]["lastCandleStart"] = ts
                logger.debug("_ensure_symbol_data_populated populated %s %s", symbol, {"lastOpen": open_p, "lastClose": close_p})
        except Exception:
            logger.exception("_ensure_symbol_data_populated error for %s", symbol)

    # -------------------------
    # MACD seeding for a symbol / tf (closed candles only)
    # -------------------------
    async def _compute_macd_for_symbol_tf(self, symbol: str, tf: str, open_price: Optional[float], lookback: int = HIST_LOOKBACK):
        """
        Return dict { prevHist, lastHist, peekOpenHist, macdObj } or None
        Drops in-progress candle by comparing the last candle start against current bucket start.
        Supports array/candle/object forms.
        """
        try:
            candles = await self._get_klines(symbol, tf, lookback)
            if not candles:
                return None

            # compute bucket start for tf
            tfsec = 24 * 3600 if str(tf).upper() == "D" else (int(tf) * 60)
            now = int(time.time())
            current_bucket_start = (now // tfsec) * tfsec if tfsec else None

            # clone and drop last if it's the live bucket
            normalized = list(candles)
            last = normalized[-1] if normalized else None
            def _start_of(item):
                if isinstance(item, (list, tuple)):
                    return int(item[0]) if len(item) > 0 else 0
                if isinstance(item, dict):
                    return int(item.get("start") or item.get("t") or item.get("open_time") or item.get("ts") or 0)
                return 0
            last_start = _start_of(last) if last is not None else 0
            if current_bucket_start is not None and last_start >= current_bucket_start:
                # drop last (live)
                normalized = normalized[:-1]

            # build closes from normalized (closed) candles
            closes = []
            for c in normalized:
                if isinstance(c, (list, tuple)):
                    try:
                        closes.append(float(c[4]))
                    except Exception:
                        continue
                elif isinstance(c, dict):
                    v = c.get("close") or c.get("c") or (c.get("k") and c.get("k").get("c"))
                    try:
                        closes.append(float(v))
                    except Exception:
                        continue
            closes = [x for x in closes if isinstance(x, (int, float))]

            if len(closes) < 2:
                return None

            macd = MACD()
            for v in closes:
                macd.update(v)
            lastHist = macd.get("hist") if hasattr(macd, "get") else None
            prevHist = macd.get("prevHist") if hasattr(macd, "get") else None

            trial_open = open_price if isinstance(open_price, (int, float)) else closes[-1]
            peek_res = macd.peek(trial_open) if hasattr(macd, "peek") else None
            peek_hist = (peek_res.get("hist") if isinstance(peek_res, dict) and "hist" in peek_res else None) if peek_res else None

            return {"prevHist": prevHist if isinstance(prevHist, (int, float)) else None,
                    "lastHist": lastHist if isinstance(lastHist, (int, float)) else None,
                    "peekOpenHist": peek_hist if isinstance(peek_hist, (int, float)) else None,
                    "macdObj": {"macd": macd.get("macd") if hasattr(macd, "get") else None,
                               "signal": macd.get("signal") if hasattr(macd, "get") else None}}
        except Exception:
            logger.exception("_compute_macd_for_symbol_tf error for %s %s", symbol, tf)
            return None

    # -------------------------
    # Closed-candle scan run
    # -------------------------
    async def run_closed_candle_pass(self):
        start_time = time.time()
        symbols = list(self.symbols)
        if not symbols:
            logger.info("run_closed_candle_pass: no symbols to scan")
            return

        logger.info("run_closed_candle_pass START: symbols=%d", len(symbols))
        root_tfs = [str(x) for x in (ROOT_TFS if isinstance(ROOT_TFS, (list, tuple)) and ROOT_TFS else ["60", "240", "D"])]
        mtf_tfs = [str(x) for x in (MTF_TFS if isinstance(MTF_TFS, (list, tuple)) else [])]
        tfs_set = set(root_tfs + mtf_tfs + ["5", "15"])
        tfs = list(tfs_set)

        # ensure symbol data populated
        await asyncio.gather(*(self._ensure_symbol_data_populated(sym) for sym in symbols))

        # compute MACD for each symbol/tf (concurrent but bounded)
        tasks = []
        for sym in symbols:
            self.passCache.setdefault(sym, {})
            last_info = self.symbolData.get(sym, {})
            open_fallback = last_info.get("lastOpen") if isinstance(last_info.get("lastOpen"), (int, float)) else last_info.get("lastClose")
            for tf in tfs:
                async def _work(s=sym, tf_local=tf, open_fb=open_fallback):
                    async with self._scan_sem:
                        res = await self._compute_macd_for_symbol_tf(s, tf_local, open_fb)
                        if res:
                            self.passCache.setdefault(s, {})[tf_local] = res
                            # emit macd_prev_result
                            try:
                                self.emit("macd_prev_result", {"symbol": s, "tf": tf_local,
                                                               "macdPrevResult": {"hist": res.get("lastHist"),
                                                                                 "prevHist": res.get("prevHist"),
                                                                                 "macd": res.get("macdObj", {}).get("macd"),
                                                                                 "signal": res.get("macdObj", {}).get("signal")}})
                                # if prevHist is a number (including 0) emit macd_ready
                                if isinstance(res.get("prevHist"), (int, float)):
                                    self.emit("macd_ready", {"symbol": s, "tf": tf_local})
                            except Exception:
                                logger.exception("emit error")
                tasks.append(asyncio.create_task(_work()))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Now detect flips on root tfs and create root signals
        created_roots = []
        eps = float(0.000001)
        abs_thresh = float(0 if not hasattr(__import__("builtins"), "MACD_HIST_POSITIVE_THRESHOLD") else 0)  # keep default zero if not configured
        pct_thresh = float(0 if not hasattr(__import__("builtins"), "MACD_HIST_POSITIVE_PCT") else 0)

        total_evaluated = total_skipped = total_flips = 0

        for sym in symbols:
            for root_tf in root_tfs:
                total_evaluated += 1
                entry = self.passCache.get(sym, {}).get(root_tf)
                if not entry:
                    total_skipped += 1
                    self.emit("root_cross_debug", {"symbol": sym, "tf": root_tf, "reason": "no_cache_entry"})
                    continue
                prevHist = entry.get("prevHist")
                lastHist = entry.get("lastHist")
                peekOpenHist = entry.get("peekOpenHist")

                # prevHist must be numeric (including 0)
                if not isinstance(prevHist, (int, float)):
                    total_skipped += 1
                    self.emit("root_cross_debug", {"symbol": sym, "tf": root_tf, "reason": "prev_not_number", "prevHist": prevHist})
                    continue

                last_price = abs(float(self.symbolData.get(sym, {}).get("lastClose") or 1))
                rel_threshold = (pct_thresh > 0 and last_price > 0) and (last_price * pct_thresh) or 0
                required_positive = 0 if (abs_thresh == 0 and pct_thresh == 0) else max(abs_thresh, rel_threshold, eps)

                hist_open = peekOpenHist if isinstance(peekOpenHist, (int, float)) else (lastHist if isinstance(lastHist, (int, float)) else None)
                if hist_open is None:
                    total_skipped += 1
                    self.emit("root_cross_debug", {"symbol": sym, "tf": root_tf, "reason": "histOpen_missing"})
                    continue

                delta = float(hist_open) - float(prevHist)
                is_flip = (prevHist < -eps) and (hist_open >= required_positive) and (delta > eps)
                if not is_flip:
                    total_skipped += 1
                    self.emit("root_cross_debug", {"symbol": sym, "tf": root_tf, "prevHist": prevHist, "histOpen": hist_open, "isFlip": False})
                    continue

                # Build root signal
                total_flips += 1
                start_sec = int(self.symbolData.get(sym, {}).get("lastCandleStart") or int(time.time()))
                id_ = str(uuid.uuid4())
                sig = {
                    "id": id_,
                    "type": "root",
                    "symbol": sym,
                    "tf": root_tf,
                    "hist": hist_open,
                    "prevHist": prevHist,
                    "macd": entry.get("macdObj", {}).get("macd"),
                    "signal": entry.get("macdObj", {}).get("signal"),
                    "strength": max(0, float(hist_open) if isinstance(hist_open, (int, float)) else 0),
                    "start": start_sec,
                    "expires": start_sec + tf_to_seconds(root_tf),
                    "status": "active",
                    "created": int(time.time() * 1000)
                }
                key = f"{sig['symbol']}|{sig['tf']}|{sig['start']}"
                prev = self.activeRootSignals.get(key)
                if prev and prev.get("id"):
                    try:
                        del self.rootIndex[prev.get("id")]
                    except Exception:
                        pass
                self.activeRootSignals[key] = sig
                self.rootIndex[id_] = key
                self.pendingSignals.append(sig)
                self._ensure_persist_loop(1000)
                self.emit("root_signal", sig)
                self.emit("root_created", {"sig": sig, "key": key})
                created_roots.append(sig)

        # evaluate MTF for roots (best-effort)
        for root in list(self.activeRootSignals.values()):
            try:
                await self._evaluate_mtf_for_root(root)
            except Exception:
                logger.exception("mtf eval error for root %s", root.get("id"))

        duration = int((time.time() - start_time) * 1000)
        logger.info("Closed-candle pass COMPLETED duration_ms=%d created_roots=%d total_evaluated=%d total_skipped=%d total_flips=%d",
                    duration, len(created_roots), total_evaluated, total_skipped, total_flips)
        self.emit("5m_scan_complete", {"durationMs": duration, "createdRootsCount": len(created_roots)})

    # -------------------------
    # Evaluate MTF for a root (simplified & defensive)
    # -------------------------
    async def _evaluate_mtf_for_root(self, root: Dict[str, Any]):
        if not root or not root.get("symbol"):
            return None
        symbol = str(root["symbol"]).upper()
        tfs = MTF_TFS if isinstance(MTF_TFS, (list, tuple)) and MTF_TFS else ["5", "15", "60", "240", "D"]
        positives = []
        negatives = []
        cumulative_strength = 0.0
        eps = 1e-6
        abs_threshold = 0.0 if not hasattr(__import__("builtins"), "MACD_HIST_POSITIVE_THRESHOLD") else float(0)

        for tf in tfs:
            data = self.passCache.get(symbol, {}).get(str(tf))
            if not data:
                # attempt compute
                last_info = self.symbolData.get(symbol, {})
                open_fb = last_info.get("lastOpen") if isinstance(last_info.get("lastOpen"), (int, float)) else last_info.get("lastClose")
                data = await self._compute_macd_for_symbol_tf(symbol, str(tf), open_fb)
                if data:
                    self.passCache.setdefault(symbol, {})[str(tf)] = data
                else:
                    negatives.append({"tf": tf, "reason": "no_data"})
                    continue
            prev = data.get("prevHist")
            last = data.get("lastHist")
            peek = data.get("peekOpenHist")
            curr = peek if isinstance(peek, (int, float)) else (last if isinstance(last, (int, float)) else None)
            pHist = prev if isinstance(prev, (int, float)) else None
            has_flip = False
            if pHist is not None and curr is not None:
                delta = float(curr) - float(pHist)
                if abs_threshold > 0:
                    if pHist < -eps and curr >= abs_threshold and delta > eps:
                        has_flip = True
                else:
                    if pHist < -eps and curr > eps and delta > eps:
                        has_flip = True
            if has_flip:
                positives.append({"tf": tf, "hist": curr, "prevHist": pHist, "hasFlip": True})
                cumulative_strength += max(0.0, float(curr))
            elif isinstance(curr, (int, float)) and curr > eps:
                positives.append({"tf": tf, "hist": curr, "prevHist": pHist, "hasFlip": False})
                cumulative_strength += max(0.0, float(curr))
            else:
                negatives.append({"tf": tf, "hist": curr, "prevHist": pHist})

        status = "partial"
        if positives and not negatives:
            status = "all_positive"
        elif len(negatives) == 1 and str(negatives[0].get("tf")) == "D":
            # daily rising check
            stD = self.passCache.get(symbol, {}).get("D")
            if stD and stD.get("lastHist") is not None and stD.get("prevHist") is not None and stD.get("lastHist") > stD.get("prevHist"):
                status = "daily_rising"

        mtfInfo = {"status": status, "positives": positives, "negatives": negatives, "cumulativeStrength": cumulative_strength, "evaluatedAt": int(time.time() * 1000)}
        # update root
        try:
            self.update_root_mtf(root.get("id"), mtfInfo)
            logger.info("mtf evaluated root=%s status=%s strength=%.4f", root.get("id"), status, cumulative_strength)
        except Exception:
            logger.exception("update_root_mtf error")

        # optionally open trades (best-effort)
        try:
            if (mtfInfo.get("status") in ("all_positive", "daily_rising")) and getattr(self, "trader", None):
                # try to open trade using trader interface (best effort - adapt as needed for your trader)
                try:
                    balance_resp_fn = getattr(self.trader, "get_wallet_balance", None) or getattr(self.trader, "getWalletBalance", None)
                    bal = 0.0
                    if balance_resp_fn:
                        br = balance_resp_fn() if not asyncio.iscoroutinefunction(balance_resp_fn) else await balance_resp_fn()
                        # attempt to extract wallet balance
                        try:
                            bal = float((br.get("result", {}).get("list", [{}])[0].get("wallet_balance")) or br.get("result", {}).get("USDT", {}).get("equity") or 0)
                        except Exception:
                            bal = 0.0
                    amount_usd = 0.0
                    if hasattr(self.trader, "position_size_from_balance"):
                        amount_usd = self.trader.position_size_from_balance(bal, len(self.trader.open_trades) + 1)
                    elif hasattr(self.trader, "positionSizeFromBalance"):
                        amount_usd = self.trader.positionSizeFromBalance(bal, len(getattr(self.trader, "open_trades", [])) + 1)
                    if hasattr(self.trader, "open_trade"):
                        if asyncio.iscoroutinefunction(self.trader.open_trade):
                            await self.trader.open_trade(symbol, "Buy", amount_usd)
                        else:
                            self.trader.open_trade(symbol, "Buy", amount_usd)
                        logger.info("Opened trade from mtf eval for %s amount=%.2f", symbol, amount_usd)
                except Exception:
                    logger.exception("attempt to open trade failed")
        except Exception:
            logger.exception("mtf trade open wrapper failed")

        return mtfInfo

    def update_root_mtf(self, root_id: str, mtf_info: Dict[str, Any]):
        key = self.rootIndex.get(root_id)
        if not key:
            return None
        sig = self.activeRootSignals.get(key)
        if not sig:
            return None
        sig["mtf"] = mtf_info
        try:
            s = read_signals() or []
            s.append({"id": str(uuid.uuid4()), "type": "mtf", "rootId": root_id, "symbol": sig["symbol"], "tf": sig["tf"], "detail": mtf_info, "created": int(time.time() * 1000)})
            write_signals(s)
        except Exception:
            logger.exception("update_root_mtf write failed")
        try:
            self.emit("root_updated", {"rootId": root_id, "sig": sig})
        except Exception:
            logger.exception("emit root_updated failed")
        return sig

    # -------------------------
    # Utility accessors
    # -------------------------
    def get_active_root_signals(self) -> List[Dict[str, Any]]:
        return list(self.activeRootSignals.values())

    def get_symbol_status(self, symbol: str) -> Dict[str, Any]:
        return {"lastPriceInfo": self.symbolData.get(symbol), "passCache": self.passCache.get(symbol)}

    # -------------------------
    # Seeding historical candles (optional)
    # -------------------------
    async def seed_historical(self):
        try:
            symbols = list(self.symbols)
            if not symbols:
                return {"ok": False, "reason": "no_symbols"}
            tfs = list(set(list(ROOT_TFS or []) + list(MTF_TFS or []) + ["5", "15"]))
            for symbol in symbols:
                for tf in tfs:
                    try:
                        candles = await self._get_klines(symbol, str(tf), HIST_LOOKBACK)
                        if not candles:
                            continue
                        last = candles[-1]
                        if isinstance(last, (list, tuple)):
                            last_close = float(last[4]) if len(last) > 4 else None
                        else:
                            last_close = float(last.get("close") or last.get("c") or 0)
                        if last_close is not None:
                            self.symbolData.setdefault(symbol, {})["lastClose"] = last_close
                    except Exception:
                        logger.exception("seed_historical per-tf error")
            logger.info("seed_historical complete")
            return {"ok": True}
        except Exception:
            logger.exception("seed_historical error")
            return {"ok": False, "error": "exception"}
