# scanner_service.py
import os
import asyncio
import time
import math
from typing import Dict, List, Any, Optional

from .logger import get_logger
from .config import (
    ROOT_TFS, MTF_TFS, ROOT_SCAN_INTERVAL, TRADE_ENABLED,
    ROOT_FILTER, MAX_OPEN_TRADES, USE_WS, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY,
    VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT
)
from .telegram import send_message
from .scanner_base import ScannerBase

logger = get_logger("scanner_service")

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

class Scanner(ScannerBase):
    def __init__(self):
        super().__init__()
        logger.info(
            "scanner initialized (USE_WS=%s CONCURRENCY=inherited DIAGNOSTIC=%s) TRADE_RATING_MIN=%.4f TV_RATING_WEIGHT=%.2f TRADE_NO_NEG_VOL=%s MARKET_CAP_MIN=%s PRIORITIZE=%s",
            bool(USE_WS), DIAGNOSTIC_MODE,
            TRADE_RATING_MIN, TV_RATING_WEIGHT, TRADE_NO_NEG_VOL, MARKET_CAP_MIN, PRIORITIZE_SLOT_ORDER
        )

    async def root_scan_loop(self):
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1
            logger.info("[DIAGNOSTIC] root_scan_loop: Beginning scan cycle #%d", loop_count)
            start = time.time()
            
            try:
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
                        await self._update_24h_volume(sym)

                        for root in ROOT_TFS:
                            logger.info("[ROOT_SCAN_CALC] %s %s: STARTING MACD calculation", sym, root)
                            macd_line, sig, hist = self.compute_macd_for(
                                sym, root, include_price=price, use_ws_current=True
                            )
                            flip = self.detect_flip_current_open(hist, 0.0, symbol=sym, tf=root)
                            logger.info("[ROOT_SCAN_RESULT] %s %s: flip_detected=%s", sym, root, flip)

                            if hist and flip:
                                vol_change = self.compute_24h_volume_change(sym)
                                start_at = None
                                try:
                                    last_candles = self.kline_store.get(sym, {}).get(root, [])
                                    if last_candles:
                                        start_at = last_candles[-1].get("start_at")
                                except Exception:
                                    start_at = None

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
                                logger.info("SIGNAL DETECTED: %s %s @ %s (tv=%s %+.3f)", sym, root, price, tv_label, tv_score)
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
                await self._check_monitored_symbols()

                full_push = False
                now_ts = time.time()
                if self._first_deploy_push:
                    full_push = True
                    for rt in ROOT_TFS:
                        try:
                            tf_seconds = self._tf_to_seconds(rt)
                            if not tf_seconds or tf_seconds <= 0:
                                continue
                            candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                            self._last_full_push_ts[rt] = candle_start
                        except Exception:
                            continue
                else:
                    for rt in ROOT_TFS:
                        try:
                            tf_seconds = self._tf_to_seconds(rt)
                            if not tf_seconds or tf_seconds <= 0:
                                continue
                            candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                            last = self._last_full_push_ts.get(rt)
                            if last != candle_start:
                                full_push = True
                                self._last_full_push_ts[rt] = candle_start
                        except Exception:
                            continue

                minimal_allowed = False
                if full_push:
                    minimal_allowed = True
                else:
                    if self._first_deploy_push:
                        minimal_allowed = True
                    else:
                        if ROOT_SCAN_INTERVAL:
                            if self._last_minimal_push_ts is None:
                                minimal_allowed = True
                            else:
                                elapsed_since_last = now_ts - (self._last_minimal_push_ts or 0.0)
                                minimal_allowed = elapsed_since_last >= max(1, float(ROOT_SCAN_INTERVAL))
                        else:
                            candle_start = (int(now_ts) // 300) * 300
                            if self._last_minimal_push_ts is None:
                                minimal_allowed = True
                            else:
                                last_candle_start = (int(self._last_minimal_push_ts) // 300) * 300
                                minimal_allowed = last_candle_start != candle_start

                logger.info("[DIAGNOSTIC] push gating full_push=%s minimal_allowed=%s first_deploy=%s", full_push, minimal_allowed, self._first_deploy_push)

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
                else:
                    logger.info("No root signals this interval.")

                await self.send_summary(root_signals, evaluated=evaluated, full_push=full_push)
                if self._first_deploy_push and full_push:
                    self._first_deploy_push = False

            except Exception:
                logger.exception("Error in root scan loop")

            elapsed = time.time() - start
            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping for %.1f seconds before next cycle", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                now = time.time()
                now_struct = time.gmtime(now)
                current_minute = now_struct.tm_min
                current_second = now_struct.tm_sec
                next_5m_minute = ((current_minute // 5) + 1) * 5

                if next_5m_minute >= 60:
                    to_sleep = (60 - current_minute) * 60 - current_second
                else:
                    to_sleep = ((next_5m_minute - current_minute) * 60) - current_second

                to_sleep = max(0, to_sleep)
                logger.debug("[DIAGNOSTIC] Aligning to next 5m candle: sleeping %.1f seconds", to_sleep)
                await asyncio.sleep(to_sleep)

    async def _check_monitored_symbols(self):
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
                    logger.info("MONITORING EXPIRED (24h): %s â€“ removing", sym)
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
                    logger.info("MONITORING RESOLVED: %s â†’ %s â€“ queuing trade open", sym, status)
                    to_remove.append(sym)
                    vol_change = self.compute_24h_volume_change(sym)
                    vol_str = "N/A"
                    if vol_change is not None:
                        try:
                            vol_str = f"{vol_change * 100:+.1f}%"
                        except Exception:
                            vol_str = str(vol_change)
                    tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                    alert_blocks.append("\n".join([
                        f"âœ… MTF Alignment RESOLVED | {root} Signal",
                        f"Symbol: {sym}",
                        f"Price: {price_str}",
                        f"MTF Status: {status}",
                        f"TV Rating: {tv_label} ({tv_score:+.3f})",
                        f"24h Vol Î”: {vol_str}",
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
                            f"â ³ MTF Alignment UPDATE | {root} Signal",
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
                if tv_score < TRADE_RATING_MIN:
                    entry["accept"] = False
                    entry["reason"] = "tv_rating_below_threshold"
                    logger.info("Trade blocked by TRADE_RATING_MIN: %s tv_score=%.4f < min=%.4f", sym, tv_score, TRADE_RATING_MIN)
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
                        logger.info("MTF %s â†’ ACCEPT: %s %s score=%.2f tv_score=%.4f (vol passed)", mtf_status, sym, root, score, tv_score)
                else:
                    if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                        entry["accept"] = False
                        entry["reason"] = "negvol_blocked"
                        logger.info("Trade blocked by TRADE_NO_NEG_VOL (vol_change=%.4f): %s", vol_change, sym)
                    else:
                        entry["accept"] = True
                        entry["reason"] = mtf_status
                        to_open.append(entry)
                        logger.info("MTF %s â†’ ACCEPT: %s %s score=%.2f tv_score=%.4f", mtf_status, sym, root, score, tv_score)

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
                    logger.info("MTF MONITORING: %s added â€“ waiting on: %s", sym, negative_tfs)

            evaluated.append(entry)

        await self._emit_event("candidates_evaluated", evaluated)

        logger.warning("[CANDIDATES_SUMMARY] Total evaluated=%d, Accepted/To Open=%d, Monitoring now=%d",
                       len(evaluated), len([e for e in evaluated if e.get("accept")]), len(self._mtf_monitoring))

        candidates = to_open

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
                top = sorted(lst, key=lambda r: self._compute_combined_score(r), reverse=True)[:remaining_slots]
                selected.extend(top)
                remaining_slots -= len(top)

            if remaining_slots > 0:
                remaining_candidates = [c for c in candidates if c not in selected]
                remaining_sorted = sorted(remaining_candidates, key=lambda r: self._compute_combined_score(r), reverse=True)[:remaining_slots]
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
                    top = sorted(lst, key=lambda r: self._compute_combined_score(r), reverse=True)[:remaining_slots]
                    selected.extend(top)
                    remaining_slots -= len(top)
                if remaining_slots > 0:
                    remaining_candidates = [c for c in candidates if c not in selected]
                    remaining_sorted = sorted(remaining_candidates, key=lambda r: self._compute_combined_score(r), reverse=True)[:remaining_slots]
                    selected.extend(remaining_sorted)
                candidates = sorted(selected, key=lambda r: self._compute_combined_score(r), reverse=True)
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

        for c in candidates:
            if not self.trade_manager.can_open():
                logger.info("Max open trades reached â€“ halting further opens.")
                break

            sym = c["symbol"]
            price = c["price"]
            vol_change = c.get("vol_change")

            if TRADE_NO_NEG_VOL and vol_change is not None and vol_change <= 0:
                logger.info("Trade blocked by TRADE_NO_NEG_VOL at final check for %s (vol_change=%.4f)", sym, vol_change)
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
            symbol_info = await self.client.get_symbol_info(sym)
            qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
            qty = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
            if qty <= 0 or math.isclose(qty, 0.0):
                logger.warning("Zero qty for %s after quantize â€“ skipping.", sym)
                c["accept"] = False
                c["reason"] = "zero_qty"
                eval = eval_map.get((c["symbol"], c["root"]))
                if eval is not None:
                    eval["accept"] = False
                    eval["reason"] = "zero_qty"
                continue

            if qty != qty_raw:
                logger.debug("Qty for %s adjusted %s â†’ %s (step=%s min=%s)", sym, qty_raw, qty, symbol_info.get("step"), symbol_info.get("min_qty"))

            side = "Buy"
            reason_tag = c.get("reason", "signal")
            if TRADE_ENABLED and self.client.api_key and self.client.api_secret:
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    eval = eval_map.get((sym, c["root"]))
                    if eval is not None:
                        eval["accept"] = True
                        eval["reason"] = "opened"
                        eval["order"] = order
                    await send_message(
                        f"âœ… Trade Opened â€“ {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                    )
                    logger.info("Real trade opened %s qty=%s combined_score=%.2f tv_score=%.4f", sym, qty, self._compute_combined_score(c), c.get('tv_score', 0.0))
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
                    c["accept"] = False
                    c["reason"] = "order_failed"
                    eval = eval_map.get((sym, c["root"]))
                    if eval is not None:
                        eval["accept"] = False
                        eval["reason"] = "order_failed"
            else:
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"], "tv_score": c.get("tv_score", 0.0)})
                logger.info("Simulated open %s qty=%s combined_score=%.2f tv_score=%.4f", sym, qty, self._compute_combined_score(c), c.get('tv_score', 0.0))
                eval = eval_map.get((sym, c["root"]))
                if eval is not None:
                    eval["accept"] = True
                    eval["reason"] = "simulated"
                    eval["simulated"] = True
                await send_message(
                    f"ðŸ”” Simulated Trade â€“ {sym} {side}\n"
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

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None, full_push: bool = False):
        now_str = time.strftime("%H:%M UTC", time.gmtime())
        now_ts = time.time()

        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        if full_push:
            self._last_minimal_push_ts = None

            try:
                if evaluated:
                    current_open = len(self.trade_manager.open_trades) if hasattr(self.trade_manager, "open_trades") else 0
                    remaining = max(0, MAX_OPEN_TRADES - current_open)
                    accepted = [e for e in evaluated if e.get("accept")]
                    accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
                    recommended = accepted_sorted[:remaining] if remaining > 0 else []

                    rec_lines = [f"ðŸ † Recommended Signals for Trading â€“ {now_str}"]
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

            try:
                tf_counts: Dict[str, int] = {}
                for sig in root_signals:
                    rt = sig.get("root", "?")
                    tf_counts[rt] = tf_counts.get(rt, 0) + 1

                window_map = {"60": 30, "240": 12, "D": 5}
                header_lines = [f"ðŸ”  Bybit Perp Root Summary â€“ {now_str}"]
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
                logger.exception("Failed to send first summary block")

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
                                f"ðŸ“Œ Bybit Perp | {rt} Signal",
                                f"Symbol: {sym}",
                                f"Price: {price_str}",
                                f"MTF Status: {mtf_str}",
                                f"TV Rating: {tv_label} ({tv_score:+.3f})",
                                f"24h Vol Î”: {vol_str}",
                            ])
                            await send_message(block)

                            self._last_root_signal_send[(sym, rt)] = now_ts
                        except Exception:
                            logger.exception("Failed to send detailed signal block for %s %s", sig.get("symbol"), sig.get("root"))
            except Exception:
                logger.exception("Failed to send per-signal detailed signal blocks")
        else:
            if not root_signals:
                return

            now_ts = time.time()

            if not self._first_deploy_push:
                if ROOT_SCAN_INTERVAL:
                    if self._last_minimal_push_ts is not None:
                        elapsed_since_last = now_ts - self._last_minimal_push_ts
                        if elapsed_since_last < max(1, float(ROOT_SCAN_INTERVAL)):
                            logger.info("[TELEGRAM_DELTA] Skipping minimal push: last push %.1fs ago < scan interval %.1fs", elapsed_since_last, float(ROOT_SCAN_INTERVAL))
                            return
                else:
                    candle_start = (int(now_ts) // 300) * 300
                    if self._last_minimal_push_ts is not None:
                        last_candle_start = (int(self._last_minimal_push_ts) // 300) * 300
                        if last_candle_start == candle_start:
                            logger.info("[TELEGRAM_DELTA] Skipping minimal push: already pushed during this 5m candle (start=%d)", candle_start)
                            return

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
                        f"ðŸ“Œ Bybit Perp | {rt} Signal",
                        f"Symbol: {sym}",
                        f"Price: {price_str}",
                        f"MTF Status: {mtf_str}",
                        f"TV Rating: {tv_label} ({tv_score:+.3f})",
                        f"24h Vol Î”: {vol_str}",
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
