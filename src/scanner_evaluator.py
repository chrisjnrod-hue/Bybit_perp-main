# scanner_evaluator.py
# Evaluates MTF alignment, applies trading gates, manages trade opens, and sends Telegram notifications

import asyncio
import time
import math
from typing import Dict, List, Any, Optional
from decimal import getcontext

getcontext().prec = 28

from .logger import get_logger
from .telegram import send_message
from .scanner_core import (
    compute_macd_from_closes,
    compute_24h_volume_change_from,
    compute_tv_rating_from,
    compute_mtf_alignment,
)
from .config import (
    TRADE_ENABLED, ROOT_TFS, MTF_TFS, MAX_OPEN_TRADES, 
    VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT,
    TRADE_NO_NEG_VOL, MARKET_CAP_MIN, TRADE_RATING_MIN, 
    TV_RATING_WEIGHT, MTF_SLOPE_LOOKBACK, PRIORITIZE_SLOT_ORDER,
    ROOT_SCAN_INTERVAL
)

logger = get_logger("scanner")

MTF_ALIGN_TFS = ["5", "15", "60", "240", "D"]


class ScannerEvaluator:
    """Evaluates root signals, applies trading gates, and manages notifications."""
    
    def __init__(self, kline_store: Dict, trade_manager, client, last_price_cache: Dict):
        self.kline_store = kline_store
        self.trade_manager = trade_manager
        self.client = client
        self._last_price_cache = last_price_cache
        self._mtf_monitoring: Dict[str, Dict[str, Any]] = {}
        self._last_root_signal_send: Dict[tuple, float] = {}
        self._last_minimal_push_ts: Optional[float] = None
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}

        logger.info(
            "evaluator initialized (TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s)",
            TRADE_RATING_MIN, TV_RATING_WEIGHT, TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER
        )

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        """
        Compute combined score from MTF alignment score + TV rating score.
        
        Formula:
          combined = (mtf_score * (1 - tv_weight)) + (tv_score * tv_weight)
        """
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - TV_RATING_WEIGHT)) + (tv_score * TV_RATING_WEIGHT)
        return combined

    def compute_macd_for(self, symbol: str, tf: str, include_price: Optional[float] = None, use_ws_current: bool = False):
        """Build closes list from kline_store and compute MACD."""
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
        elif use_ws_current:
            try:
                ws_last = self.client.get_ws_latest_kline(symbol, tf)
                if ws_last and ws_last.get("close") is not None:
                    current_price = float(ws_last.get("close"))
            except Exception:
                pass

        return compute_macd_from_closes(closes, include_price=current_price)

    def compute_24h_volume_change(self, symbol: str, vol_data: Dict) -> Optional[float]:
        """Compute 24h volume change from stored data."""
        return compute_24h_volume_change_from(vol_data.get(symbol))

    def compute_tv_rating(self, symbol: str, tf: str, price: Optional[float] = None):
        """Compute TV rating for symbol at timeframe."""
        klines = self.kline_store.get(symbol, {}).get(tf, [])
        return compute_tv_rating_from(klines, {"enabled": True, "indicators": {}, "weights": {}, "tolerance": {}, "thresholds": {}}, tf=tf, price=price)

    def _compute_mtf_alignment(self, symbol: str, price: float):
        """Compute MTF alignment status."""
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

    async def _check_monitored_symbols(self, vol_data: Dict):
        """Check monitored symbols for MTF alignment resolution."""
        if not self._mtf_monitoring:
            return

        MONITORING_MAX_AGE = 86400
        now = time.time()
        to_remove: List[str] = []
        newly_aligned: List[Dict[str, Any]] = []
        alert_blocks: List[str] = []

        for sym, info in list(self._mtf_monitoring.items()):
            try:
                if now - info.get("started_at", now) > MONITORING_MAX_AGE:
                    logger.info("MONITORING EXPIRED (24h): %s — removing", sym)
                    to_remove.append(sym)
                    continue

                price = self._last_price_cache.get(sym)
                if price is None:
                    try:
                        price = await self.client.get_latest_price(sym)
                        if price:
                            self._last_price_cache[sym] = price
                    except Exception:
                        pass
                if price is None:
                    continue

                if price >= 1000:
                    price_str = f"${price:,.2f}"
                elif price >= 1:
                    price_str = f"${price:.4f}"
                else:
                    price_str = f"${price:.8f}"

                mtf_align = self._compute_mtf_alignment(sym, price)
                status = mtf_align["status"]
                root = info.get("root", "?")

                if status in ("aligned", "daily_rising"):
                    logger.info("MONITORING RESOLVED: %s → %s — queuing trade open", sym, status)
                    to_remove.append(sym)
                    vol_change = self.compute_24h_volume_change(sym, vol_data)
                    vol_str = "N/A"
                    if vol_change is not None:
                        try:
                            vol_str = f"{vol_change * 100:+.1f}%"
                        except Exception:
                            vol_str = str(vol_change)
                    tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                    alert_blocks.append("\n".join([
                        f"✅ MTF Alignment RESOLVED | {root} Signal",
                        f"Symbol: {sym}",
                        f"Price: {price_str}",
                        f"MTF Status: {status}",
                        f"TV Rating: {tv_label} ({tv_score:+.3f})",
                        f"24h Vol Δ: {vol_str}",
                    ]))
                    newly_aligned.append({
                        "symbol": sym,
                        "root": info["root"],
                        "price": price,
                        "hist": [],
                        "vol_change": vol_change,
                        "tv_score": tv_score,
                        "tv_label": tv_label,
                        "from_monitoring": True,
                    })
                else:
                    prev_neg = set(info.get("negative_tfs", []))
                    curr_neg = set(mtf_align.get("negative_tfs", []))
                    if curr_neg != prev_neg:
                        self._mtf_monitoring[sym]["negative_tfs"] = list(curr_neg)
                        self._mtf_monitoring[sym]["last_alert"] = now
                        improved = prev_neg - curr_neg
                        still_neg = ", ".join(sorted(curr_neg)) if curr_neg else "none"
                        improved_str = ", ".join(sorted(improved)) if improved else "n/a"
                        alert_blocks.append("\n".join([
                            f"⏳ MTF Alignment UPDATE | {root} Signal",
                            f"Symbol: {sym}",
                            f"Price: {price_str}",
                            f"MTF Status: monitoring (neg: {still_neg})",
                            f"Improved TFs: {improved_str}",
                        ]))
            except Exception:
                logger.exception("Error checking monitored symbol %s", sym)

        for sym in to_remove:
            self._mtf_monitoring.pop(sym, None)

        for block in alert_blocks:
            try:
                await send_message(block)
            except Exception:
                logger.exception("Failed to send MTF monitoring alert block")

        if newly_aligned:
            await self.handle_root_signals(newly_aligned, allow_open_trades=True)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]], vol_data: Dict, allow_open_trades: bool = True) -> List[Dict[str, Any]]:
        """Evaluate MTF alignment and apply trading gates."""
        evaluated: List[Dict[str, Any]] = []
        to_open: List[Dict[str, Any]] = []

        for item in root_signals:
            sym = item["symbol"]
            price = item["price"]
            root = item["root"]
            vol_change = item.get("vol_change")
            tv_label = item.get("tv_label")
            tv_score = item.get("tv_score", 0.0)

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
                # Numeric TV rating filter
                if tv_score < TRADE_RATING_MIN:
                    entry["accept"] = False
                    entry["reason"] = "tv_rating_below_threshold"
                    logger.info("Trade blocked by TRADE_RATING_MIN: %s tv_score=%.4f < min=%.4f", sym, tv_score, TRADE_RATING_MIN)
                    evaluated.append(entry)
                    continue

                # Market cap filter
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

                # Volume gate
                if VOLUME_FILTER_ENABLED:
                    if vol_change is None:
                        entry["accept"] = False
                        entry["reason"] = "vol_filter_blocked"
                        logger.info("Volume gate blocked open (no vol_change): %s", sym)
                    elif vol_change < VOLUME_MIN_CHANGE_PCT:
                        entry["accept"] = False
                        entry["reason"] = "vol_filter_blocked"
                        logger.info("Volume gate blocked open (vol_change %.3f < threshold %.3f): %s", vol_change, VOLUME_MIN_CHANGE_PCT, sym)
                    else:
                        entry["accept"] = True
                        entry["reason"] = mtf_status
                        to_open.append(entry)
                        logger.info("MTF %s → ACCEPT: %s %s score=%.2f tv_score=%.4f (vol passed)", mtf_status, sym, root, score, tv_score)
                else:
                    # Enforce TRADE_NO_NEG_VOL if configured
                    if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                        entry["accept"] = False
                        entry["reason"] = "negvol_blocked"
                        logger.info("Trade blocked by TRADE_NO_NEG_VOL (vol_change=%.4f): %s", vol_change, sym)
                    else:
                        entry["accept"] = True
                        entry["reason"] = mtf_status
                        to_open.append(entry)
                        logger.info("MTF %s → ACCEPT: %s %s score=%.2f tv_score=%.4f", mtf_status, sym, root, score, tv_score)

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
                    logger.info("MTF MONITORING: %s added — waiting on: %s", sym, negative_tfs)

            evaluated.append(entry)

        logger.warning("[CANDIDATES_SUMMARY] Total evaluated=%d, Accepted/To Open=%d, Monitoring now=%d",
                       len(evaluated), len([e for e in evaluated if e.get("accept")]), len(self._mtf_monitoring))

        candidates = to_open

        # Prioritization & slot management
        if ROOT_FILTER := getattr(self, '_root_filter', False):  # Placeholder - set from config
            # ... (preserve original ROOT_FILTER logic)
            pass
        else:
            candidates = sorted(candidates, key=lambda r: self._compute_combined_score(r), reverse=True)

        current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
        logger.info("Candidates to open after prioritization: %d (MAX_OPEN_TRADES=%d, currently_open=%d)",
                    len(candidates), MAX_OPEN_TRADES, current_open)

        eval_map: Dict[tuple, Dict[str, Any]] = {(e["symbol"], e["root"]): e for e in evaluated}

        if not allow_open_trades:
            for c in candidates:
                c["open_suppressed"] = True
                eval = eval_map.get((c["symbol"], c["root"]))
                if eval is not None:
                    eval["open_suppressed"] = True
                    eval["accept"] = False
                    eval["reason"] = "open_suppressed"
                logger.info("Open suppressed (gating) for %s %s combined_score=%.2f", c["symbol"], c["root"], self._compute_combined_score(c))
            return evaluated

        # Perform openings
        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Max open trades reached — halting further opens.")
                break

            sym = c["symbol"]
            price = c["price"]
            vol_change = c.get("vol_change")

            if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                logger.info("Trade blocked by TRADE_NO_NEG_VOL at final check for %s (vol_change=%.4f)", sym, vol_change)
                c["accept"] = False
                c["reason"] = "negvol_blocked"
                continue

            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None
            
            symbol_info = await self.client.get_symbol_info(sym)
            qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
            from .scanner_core import quantize_qty
            qty = quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
            
            if qty <= 0 or math.isclose(qty, 0.0):
                logger.warning("Zero qty for %s after quantize — skipping.", sym)
                c["accept"] = False
                c["reason"] = "zero_qty"
                continue

            side = "Buy"
            reason_tag = c.get("reason", "signal")
            
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    await send_message(
                        f"✅ Trade Opened — {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                    )
                    logger.info("Real trade opened %s qty=%s combined_score=%.2f tv_score=%.4f", sym, qty, self._compute_combined_score(c), c.get('tv_score', 0.0))
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
                    c["accept"] = False
                    c["reason"] = "order_failed"
            else:
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"], "tv_score": c.get("tv_score", 0.0)})
                logger.info("Simulated open %s qty=%s combined_score=%.2f tv_score=%.4f", sym, qty, self._compute_combined_score(c), c.get('tv_score', 0.0))
                await send_message(
                    f"📝 Simulated Trade — {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                )

            self._mtf_monitoring.pop(sym, None)

        return evaluated

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None, full_push: bool = False):
        """Send Telegram summary with proper gating and formatting."""
        now_str = time.strftime("%H:%M UTC", time.gmtime())
        now_ts = time.time()

        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        if full_push:
            self._last_minimal_push_ts = None

            # Recommended block
            try:
                if evaluated:
                    current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
                    remaining = max(0, MAX_OPEN_TRADES - current_open)
                    accepted = [e for e in evaluated if e.get("accept")]
                    accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
                    recommended = accepted_sorted[:remaining] if remaining > 0 else []

                    rec_lines = [f"🏆 Recommended Signals for Trading — {now_str}"]
                    rec_lines.append(f"Open trades: {current_open} / {MAX_OPEN_TRADES}")
                    rec_lines.append(f"Slots available: {remaining}")
                    if not recommended:
                        rec_lines.append("No recommended signals to open at this time.")
                    else:
                        for r in recommended:
                            sym = r["symbol"]
                            rt = r["root"]
                            price = r["price"]
                            combined = self._compute_combined_score(r)
                            score = r.get("score", 0.0)
                            tv_score = r.get("tv_score", 0.0)
                            if price >= 1000:
                                price_str = f"${price:,.2f}"
                            elif price >= 1:
                                price_str = f"${price:.4f}"
                            else:
                                price_str = f"${price:.8f}"
                            rec_lines.append(f"  - {sym} | {rt} | {price_str} | combined={combined:.2f} (mtf={score:.2f}, tv={tv_score:+.3f})")

                    await send_message("\n".join(rec_lines))
            except Exception:
                logger.exception("Failed to send recommended signals block")

            # Summary block
            try:
                tf_counts: Dict[str, int] = {}
                for sig in root_signals:
                    rt = sig.get("root", "?")
                    tf_counts[rt] = tf_counts.get(rt, 0) + 1

                window_map = {"60": 30, "240": 12, "D": 5}
                header_lines = [f"📊 Bybit Perp Root Summary — {now_str}"]
                for rt in ROOT_TFS:
                    cnt = tf_counts.get(rt, 0)
                    win = window_map.get(rt)
                    if cnt:
                        if win:
                            header_lines.append(f"  {rt}: {cnt} (window: {win})")
                        else:
                            header_lines.append(f"  {rt}: {cnt}")
                header_lines.append("")
                header_lines.append("Signals (all root TF signals):")
                if not root_signals:
                    header_lines.append("  None")
                else:
                    for sig in sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", ""))):
                        sym = sig.get("symbol")
                        rt = sig.get("root")
                        price = sig.get("price", 0)
                        try:
                            if price >= 1000:
                                price_str = f"${price:,.2f}"
                            elif price >= 1:
                                price_str = f"${price:.4f}"
                            else:
                                price_str = f"${price:.8f}"
                        except Exception:
                            price_str = str(price)
                        header_lines.append(f"  - {sym} | {rt} | {price_str}")

                await send_message("\n".join(header_lines))
            except Exception:
                logger.exception("Failed to send summary block")

            # Per-signal detailed blocks
            try:
                if root_signals:
                    sorted_signals = sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))

                    for sig in sorted_signals:
                        try:
                            sym        = sig.get("symbol")
                            rt         = sig.get("root")
                            price      = sig.get("price", 0)
                            vol_change = sig.get("vol_change")
                            tv_score   = sig.get("tv_score", 0.0)
                            tv_label   = sig.get("tv_label", "Neutral")
                            eval_entry = eval_map.get((sym, rt), {})

                            mtf_status = eval_entry.get("mtf_status", "N/A")
                            negative_tfs = eval_entry.get("negative_tfs", [])
                            mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"

                            if price >= 1000:
                                price_str = f"${price:,.2f}"
                            elif price >= 1:
                                price_str = f"${price:.4f}"
                            else:
                                price_str = f"${price:.8f}"

                            vol_str = "N/A"
                            if vol_change is not None:
                                try:
                                    vol_str = f"{vol_change * 100:+.1f}%"
                                except Exception:
                                    vol_str = str(vol_change)

                            block = "\n".join([
                                f"📌 Bybit Perp | {rt} Signal",
                                f"Symbol: {sym}",
                                f"Price: {price_str}",
                                f"MTF Status: {mtf_str}",
                                f"TV Rating: {tv_label} ({tv_score:+.3f})",
                                f"24h Vol Δ: {vol_str}",
                            ])
                            await send_message(block)

                            self._last_root_signal_send[(sym, rt)] = now_ts
                        except Exception:
                            logger.exception("Failed to send detailed signal block for %s %s", sig.get("symbol"), sig.get("root"))
            except Exception:
                logger.exception("Failed to send per-signal detailed signal blocks")
        else:
            # Minimal push: only new signals
            if not root_signals:
                return

            # Apply gating
            if not self._first_deploy_push:
                if ROOT_SCAN_INTERVAL:
                    if self._last_minimal_push_ts is not None:
                        elapsed_since_last = now_ts - self._last_minimal_push_ts
                        if elapsed_since_last < max(1, float(ROOT_SCAN_INTERVAL)):
                            logger.info("[TELEGRAM_DELTA] Skipping minimal push: last push %.1fs ago < scan interval %.1fs", elapsed_since_last, float(ROOT_SCAN_INTERVAL))
                            return

            # Clean up old entries (TTL)
            signal_ttl = 3600
            for key in list(self._last_root_signal_send.keys()):
                if now_ts - self._last_root_signal_send[key] > signal_ttl:
                    self._last_root_signal_send.pop(key, None)

            to_send = []
            for sig in root_signals:
                sym = sig.get("symbol")
                rt = sig.get("root")
                key = (sym, rt)

                if key not in self._last_root_signal_send:
                    to_send.append(sig)
                    logger.info("[TELEGRAM_DELTA] NEW signal queued: %s %s", sym, rt)

            if not to_send:
                return

            sent_any = False
            sent_ts = None
            for sig in to_send:
                try:
                    sym = sig.get("symbol")
                    rt = sig.get("root")
                    price = sig.get("price")
                    vol_change = sig.get("vol_change")
                    tv_score = sig.get("tv_score", 0.0)
                    tv_label = sig.get("tv_label", "Neutral")
                    eval_entry = eval_map.get((sym, rt), {})

                    mtf_status = eval_entry.get("mtf_status", "N/A")
                    negative_tfs = eval_entry.get("negative_tfs", [])
                    mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"

                    if price >= 1000:
                        price_str = f"${price:,.2f}"
                    elif price >= 1:
                        price_str = f"${price:.4f}"
                    else:
                        price_str = f"${price:.8f}"

                    vol_str = "N/A"
                    if vol_change is not None:
                        try:
                            vol_str = f"{vol_change * 100:+.1f}%"
                        except Exception:
                            vol_str = str(vol_change)

                    block = "\n".join([
                        f"📌 Bybit Perp | {rt} Signal",
                        f"Symbol: {sym}",
                        f"Price: {price_str}",
                        f"MTF Status: {mtf_str}",
                        f"TV Rating: {tv_label} ({tv_score:+.3f})",
                        f"24h Vol Δ: {vol_str}",
                    ])
                    await send_message(block)
                    self._last_root_signal_send[(sym, rt)] = now_ts
                    sent_any = True
                    sent_ts = now_ts
                    logger.info("[TELEGRAM_SENT] %s %s", sym, rt)
                except Exception:
                    logger.exception("Failed to send minimal signal block for %s %s", sig.get("symbol"), rt)

            if sent_any:
                self._last_minimal_push_ts = float(sent_ts)
