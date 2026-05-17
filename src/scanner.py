import asyncio
import time
from collections import defaultdict
from typing import Dict, List, Any, Optional
from decimal import Decimal, ROUND_DOWN, getcontext
import math

from .logger import get_logger
from .bybit_client import BybitClient
from .macd import macd_histogram, slope
from .config import (
    EXCLUDE_STABLECOINS, CONCURRENCY, KLINE_SEED_LIMIT,
    ROOT_TFS, MTF_TFS, ROOT_SCAN_INTERVAL, TRADE_ENABLED,
    MTF_SLOPE_LOOKBACK, ROOT_FILTER, ROOT_TOP_N, MTF_FILTER, MAX_OPEN_TRADES
)
from .telegram import send_message
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket

getcontext().prec = 28
logger = get_logger("scanner")

class Scanner:
    def __init__(self):
        # create a token-bucket limiter and pass to client
        self.rate_limiter = TokenBucket(max(1.0, float(1)))  # placeholder; will be replaced by config in main
        self.client = BybitClient(rate_limiter=self.rate_limiter)
        self.trade_manager = TradeManager()
        self.concurrent_sem = asyncio.Semaphore(max(1, CONCURRENCY))
        self.kline_store: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(dict)
        self.symbols: List[str] = []
        self._stop = False
        self._task: Optional[asyncio.Task] = None

    async def discover_symbols(self):
        logger.info("Discovering symbols from Bybit...")
        items = await self.client.get_symbols()
        syms = []
        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = it.get("name") or it.get("symbol") or (it.get("baseCoin") and it.get("quoteCoin") and f"{it.get('baseCoin')}{it.get('quoteCoin')}")
            if not symbol:
                symbol = it.get("instrument_name") or it.get("symbolName")
            if not symbol:
                continue
            expiry = it.get("expiry_time") or it.get("deliveryTime") or it.get("expiry")
            if expiry:
                continue
            symbol = symbol.upper()
            if not symbol.endswith("USDT"):
                continue
            base = symbol.replace("USDT", "")
            if base in [s.upper() for s in EXCLUDE_STABLECOINS]:
                continue
            syms.append(symbol)
        syms = sorted(set(syms))
        logger.info("Discovered %d USDT perpetual symbols", len(syms))
        self.symbols = syms
        return syms

    async def seed_klines_for_symbol(self, symbol: str):
        if KLINE_SEED_LIMIT < 100:
            logger.warning("KLINE_SEED_LIMIT is low (%d); MACD stability may be degraded. Consider >=200", KLINE_SEED_LIMIT)
        tfs = list(set(ROOT_TFS + MTF_TFS))
        for tf in tfs:
            try:
                klines = await self.client.get_klines(symbol, tf, limit=KLINE_SEED_LIMIT)
                if klines:
                    klines_sorted = sorted(klines, key=lambda x: x.get("start_at", 0))
                    self.kline_store[symbol][tf] = klines_sorted
                    logger.debug("Seeded %s %s candles=%d", symbol, tf, len(klines_sorted))
            except Exception:
                logger.exception("Seed klines failed for %s %s", symbol, tf)

    async def seed_all(self):
        logger.info("Seeding klines for all symbols (concurrent=%d)", CONCURRENCY)
        async def worker(sym: str):
            await self.concurrent_sem.acquire()
            try:
                await self.seed_klines_for_symbol(sym)
            finally:
                self.concurrent_sem.release()
        tasks = [asyncio.create_task(worker(s)) for s in self.symbols]
        await asyncio.gather(*tasks)

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None):
        data = self.kline_store.get(symbol, {}).get(tf, [])
        closes = [float(c.get("close", 0)) for c in data if "close" in c]
        if include_price is not None:
            closes = closes + [float(include_price)]
        macd_line, signal_line, hist = macd_histogram(closes)
        return macd_line, signal_line, hist

    def detect_flip_current_open(self, hist: List[float], hist_threshold: float = 0.0):
        if not hist or len(hist) < 2:
            return False
        prev = hist[-2]
        cur = hist[-1]
        if prev is None or cur is None:
            return False
        return (prev < 0) and (cur > hist_threshold)

    def compute_24h_volume_change(self, symbol: str) -> Optional[float]:
        data = self.kline_store.get(symbol, {}).get("1h") or []
        if not data or len(data) < 48:
            logger.debug("Insufficient 1h candles for 24h vol change for %s (have %d)", symbol, len(data))
            return None
        vols = [float(c.get("volume", 0.0)) for c in data]
        last_24 = sum(vols[-24:])
        prev_24 = sum(vols[-48:-24])
        if prev_24 == 0:
            return None
        return (last_24 / prev_24) - 1.0

    def _quantize_qty(self, qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
        if qty is None:
            return 0.0
        qty_d = Decimal(str(qty))
        if step is None or step <= 0:
            if min_qty and qty_d < Decimal(str(min_qty)):
                logger.debug("Qty below min_qty, bumping to min_qty %s", min_qty)
                return float(Decimal(str(min_qty)))
            return float(qty_d)
        step_d = Decimal(str(step))
        mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
        quant = (mult * step_d)
        if min_qty is not None:
            min_d = Decimal(str(min_qty))
            if quant < min_d:
                logger.debug("Quantized qty %s below min_qty %s, using min_qty", float(quant), float(min_d))
                quant = min_d
        try:
            quant = quant.normalize()
        except Exception:
            pass
        return float(quant)

    async def root_scan_loop(self):
        logger.info("Starting root scan loop interval=%s", ROOT_SCAN_INTERVAL)
        while not self._stop:
            start = time.time()
            try:
                if not self.symbols:
                    await self.discover_symbols()
                    await self.seed_all()

                root_signals: List[Dict[str, Any]] = []

                async def check_symbol(sym: str):
                    await self.concurrent_sem.acquire()
                    try:
                        price = await self.client.get_latest_price(sym)
                        if price is None:
                            logger.debug("No latest price for %s", sym)
                            return
                        for root in ROOT_TFS:
                            macd_line, sig, hist = self.compute_macd_for(sym, root, include_price=price)
                            if self.detect_flip_current_open(hist, 0.0):
                                vol_change = self.compute_24h_volume_change(sym)
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change
                                })
                    except Exception:
                        logger.exception("Error checking symbol %s", sym)
                    finally:
                        self.concurrent_sem.release()

                tasks = [asyncio.create_task(check_symbol(s)) for s in self.symbols]
                await asyncio.gather(*tasks)

                logger.info("Root scan found %d signals", len(root_signals))
                if root_signals:
                    await self.handle_root_signals(root_signals)
                else:
                    logger.info("No root signals this interval.")
                await self.send_summary(root_signals)
            except Exception:
                logger.exception("Error in root scan loop")
            elapsed = time.time() - start
            to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
            await asyncio.sleep(to_sleep)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]]):
        evaluated = []
        for item in root_signals:
            sym = item["symbol"]
            price = item["price"]
            root = item["root"]
            vol_change = item.get("vol_change")
            mtf_state = {}
            positive_count = 0
            any_positive_mtfflip = False
            for tf in MTF_TFS:
                macd_line, sig, h = self.compute_macd_for(sym, tf, include_price=price)
                cur_hist = h[-1] if h and len(h) >= 1 else None
                prev_hist = h[-2] if h and len(h) >= 2 else None
                mtf_state[tf] = {"prev": prev_hist, "cur": cur_hist}
                if cur_hist is not None and cur_hist > 0:
                    positive_count += 1
                if prev_hist is not None and prev_hist < 0 and cur_hist is not None and cur_hist > 0:
                    any_positive_mtfflip = True
            one_d_slope = None
            if mtf_state.get("1d") and mtf_state["1d"]["cur"] is not None:
                _, _, full_hist = self.compute_macd_for(sym, "1d", include_price=price)
                one_d_slope = slope(full_hist or [], lookback=MTF_SLOPE_LOOKBACK)
            # scoring
            score = float(positive_count)
            if any_positive_mtfflip:
                score += 1.0
            if vol_change is not None and vol_change > 0:
                score += min(vol_change, 1.0)
            if MTF_FILTER:
                positive_rising_count = 0
                for tf, vals in mtf_state.items():
                    cur = vals.get("cur")
                    prev = vals.get("prev")
                    if cur is not None and prev is not None and cur > prev and cur > 0:
                        positive_rising_count += 1
                score += positive_rising_count * 0.8
                one_d = mtf_state.get("1d")
                if one_d and one_d["cur"] is not None and one_d["cur"] < 0:
                    if one_d_slope is not None and one_d_slope > 0:
                        score += 0.5
            evaluated.append({
                "symbol": sym,
                "root": root,
                "price": price,
                "mtf": mtf_state,
                "positive_count": positive_count,
                "vol_change": vol_change,
                "one_d_slope": one_d_slope,
                "accept": True,
                "reason": "candidate",
                "score": score
            })

        candidates = [e for e in evaluated if e["accept"]]
        if ROOT_FILTER:
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for c in candidates:
                grouped.setdefault(c["root"], []).append(c)
            selected: List[Dict[str, Any]] = []
            for root in ROOT_TFS:
                lst = grouped.get(root, [])
                if not lst:
                    continue
                top = sorted(lst, key=lambda r: r["score"], reverse=True)[:ROOT_TOP_N]
                selected.extend(top)
            candidates = sorted(selected, key=lambda r: (r["score"], r["positive_count"]), reverse=True)
        else:
            pass

        current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
        logger.info("Opening candidates count=%d (MAX_OPEN_TRADES=%d, currently_open=%d)", len(candidates), MAX_OPEN_TRADES, current_open)

        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Reached max open trades; stopping opens.")
                break
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
                continue
            if qty != qty_raw:
                logger.debug("Qty for %s adjusted from %s to %s (step=%s min=%s)", sym, qty_raw, qty, symbol_info.get("step"), symbol_info.get("min_qty"))
            side = "Buy"
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    await send_message(f"Opened trade {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f}")
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
            else:
                t = self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"]})
                logger.info("Simulated open %s qty=%s score=%.2f", sym, qty, c["score"])
                await send_message(f"Simulated open {sym} {side} @ {price} qty={qty:.6f} score={c['score']:.2f} reason={c['reason']}")

    async def send_summary(self, root_signals: List[Dict[str, Any]]):
        if not root_signals:
            await send_message("Root scan: no signals this interval.")
            return
        grouped = {}
        for it in root_signals:
            grouped.setdefault(it["root"], []).append((it["symbol"], it["price"], it.get("vol_change")))
        lines = []
        lines.append(f"Root scan summary ({len(root_signals)} signals)")
        for rt in ROOT_TFS:
            lst = grouped.get(rt, [])
            if not lst:
                continue
            lines.append(f"\nRoot {rt} signals:")
            for s, p, v in lst:
                if v is None:
                    lines.append(f"- {s} @ {p}")
                else:
                    lines.append(f"- {s} @ {p} (24h vol Δ {v:.2f})")
        open_sum = self.trade_manager.summary()
        if open_sum:
            lines.append("\nOpen trades:")
            for ot in open_sum:
                lines.append(f"- {ot['symbol']} {ot['qty']} @ {ot['entry']}")
        text = "\n".join(lines)
        await send_message(text)

    async def run(self):
        # start as a cancellable task
        self._task = asyncio.create_task(self.root_scan_loop())
        try:
            await self._task
        except asyncio.CancelledError:
            logger.info("Scanner run cancelled")
        finally:
            await self.client.close()

    def stop(self):
        logger.info("Stopping scanner...")
        self._stop = True
        if self._task and not self._task.done():
            self._task.cancel()
