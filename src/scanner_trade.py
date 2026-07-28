# scanner_trade.py
# Trade evaluation, scoring and trade opening logic (moved out of the large scanner module).
import math
import time
import asyncio
from typing import Dict, Any, List, Optional, Callable

from .logger import get_logger
from .config import (
    TRADE_ENABLED, TRADE_NO_NEG_VOL, VOLUME_FILTER_ENABLED, VOLUME_MIN_CHANGE_PCT,
    TRADE_RATING_MIN, TRADE_RATING_PRIORITIZE, ROOT_FILTER, MAX_OPEN_TRADES,
    PRIORITIZE_SLOT_ORDER, ROOT_TFS, TV_RATING_WEIGHT
)
from .telegram import send_message

logger = get_logger("scanner.trade")

class TradeEvaluator:
    """
    Encapsulates scoring and trade-opening logic. Designed to be passed the functions
    it needs from the scan module to keep concerns separated.
    """
    def __init__(
        self,
        client,
        trade_manager,
        quantize_qty_fn: Callable[[float, Optional[float], Optional[float]], float],
        compute_macd_for_fn: Callable[[str, str, Optional[float], Optional[bool]], Any],
        compute_24h_volume_change_fn: Callable[[str], Optional[float]],
        compute_mtf_alignment_fn: Callable[[str, float], Dict[str, Any]],
        compute_tv_rating_fn: Callable[[str, str, Optional[float]], Any],
        send_message_fn: Optional[Callable[[str], Any]] = None,
    ):
        self.client = client
        self.trade_manager = trade_manager
        self._quantize_qty = quantize_qty_fn
        self._compute_macd_for = compute_macd_for_fn
        self._compute_24h_volume_change = compute_24h_volume_change_fn
        self._compute_mtf_alignment = compute_mtf_alignment_fn
        self._compute_tv_rating = compute_tv_rating_fn
        self.send_message_fn = send_message_fn or send_message

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - TV_RATING_WEIGHT)) + (tv_score * TV_RATING_WEIGHT)
        return combined

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
                _, _, hist = self._compute_macd_for(sym, root, include_price=price)
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
                # TV rating filter
                try:
                    min_val = float(TRADE_RATING_MIN)
                except Exception:
                    try:
                        min_val = float(TRADE_RATING_MIN)
                    except Exception:
                        min_val = 0.0
                if TRADE_RATING_MIN and tv_score < TRADE_RATING_MIN:
                    entry["accept"] = False
                    entry["reason"] = f"tv_rating_below_threshold_{tv_score:.4f}"
                    logger.info("Trade blocked by TRADE_RATING_MIN: %s tv_score=%.4f < min=%.4f", sym, tv_score, TRADE_RATING_MIN)
                    evaluated.append(entry)
                    continue

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

            evaluated.append(entry)

        # Prioritize by TV rating if enabled
        candidates = to_open
        if TRADE_RATING_PRIORITIZE and candidates:
            candidates = sorted(candidates, key=lambda c: c.get("tv_score", 0.0), reverse=True)
            logger.info("Sorted %d candidates by TV rating (highest first)", len(candidates))

        # Slot and root filtering logic: (kept similar structure to prior)
        if ROOT_FILTER:
            grouped = {}
            for c in candidates:
                grouped.setdefault(c["root"], []).append(c)

            selected = []
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
                selected = []
                grouped = {}
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

        # If opens suppressed, mark and return
        if not allow_open_trades:
            for c in candidates:
                c["open_suppressed"] = True
                eval_present = next((e for e in evaluated if e["symbol"] == c["symbol"] and e["root"] == c["root"]), None)
                if eval_present is not None:
                    eval_present["open_suppressed"] = True
                    eval_present["accept"] = False
                    eval_present["reason"] = "open_suppressed"
            return evaluated

        # Open trades (or simulate)
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
                eval_present = next((e for e in evaluated if e["symbol"] == c["symbol"] and e["root"] == c["root"]), None)
                if eval_present is not None:
                    eval_present["accept"] = False
                    eval_present["reason"] = "negvol_blocked"
                continue

            try:
                balance = await self.client.get_balance("USDT")
            except Exception:
                balance = None
            try:
                symbol_info = await self.client.get_symbol_info(sym)
            except Exception:
                symbol_info = {}
            qty_raw = self.trade_manager.compute_qty_from_balance(balance, price, symbol_info)
            qty = self._quantize_qty(qty_raw, symbol_info.get("step"), symbol_info.get("min_qty"))
            if qty <= 0 or math.isclose(qty, 0.0):
                c["accept"] = False
                c["reason"] = "zero_qty"
                eval_present = next((e for e in evaluated if e["symbol"] == c["symbol"] and e["root"] == c["root"]), None)
                if eval_present is not None:
                    eval_present["accept"] = False
                    eval_present["reason"] = "zero_qty"
                continue

            side = "Buy"
            reason_tag = c.get("reason", "signal")
            if TRADE_ENABLED and getattr(self.client, "api_key", None) and getattr(self.client, "api_secret", None):
                try:
                    order = await self.client.create_order(sym, side, qty)
                    self.trade_manager.open_trade(sym, side, price, qty, {"order": order})
                    eval_present = next((e for e in evaluated if e["symbol"] == sym and e["root"] == c["root"]), None)
                    if eval_present is not None:
                        eval_present["accept"] = True
                        eval_present["reason"] = "opened"
                        eval_present["order"] = order
                    # send message
                    await self.send_message_fn(
                        f"✅ Trade Opened – {sym} {side}\n"
                        f"Price: {price} | Qty: {qty:.6f}\n"
                        f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                    )
                except Exception:
                    logger.exception("Failed to place order for %s", sym)
                    c["accept"] = False
                    c["reason"] = "order_failed"
                    eval_present = next((e for e in evaluated if e["symbol"] == sym and e["root"] == c["root"]), None)
                    if eval_present is not None:
                        eval_present["accept"] = False
                        eval_present["reason"] = "order_failed"
            else:
                # Simulate
                self.trade_manager.open_trade(sym, side, price, qty, {"simulated": True, "score": c["score"], "tv_score": c.get("tv_score", 0.0)})
                eval_present = next((e for e in evaluated if e["symbol"] == sym and e["root"] == c["root"]), None)
                if eval_present is not None:
                    eval_present["accept"] = True
                    eval_present["reason"] = "simulated"
                    eval_present["simulated"] = True
                await self.send_message_fn(
                    f"🔔 Simulated Trade – {sym} {side}\n"
                    f"Price: {price} | Qty: {qty:.6f}\n"
                    f"Combined Score: {self._compute_combined_score(c):.2f} | TV Rating: {c.get('tv_score', 0.0):+.3f} | Reason: {reason_tag}"
                )

        return evaluated
