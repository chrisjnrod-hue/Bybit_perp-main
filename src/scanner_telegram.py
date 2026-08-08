# scanner_telegram.py
# Telegram messaging, state management, and message formatting.
# Full-push: Recommended -> Root Summary (with counts breakdown) -> A→Z one-block-per-signal
# Scan-interval: Only new root signals, one-block-per-signal
# Defensive: normalized keys for dedupe and lookup, robust fallbacks.

import time
from typing import Dict, List, Any, Optional, Tuple
from .logger import get_logger
from .config import ROOT_TFS, MAX_OPEN_TRADES
from .telegram import send_message

logger = get_logger("scanner_telegram")


def _normalize_key(symbol: Any, root: Any) -> Tuple[str, str]:
    """Return normalized (symbol, root) tuple for consistent lookup/storage."""
    return ("" if symbol is None else str(symbol), "" if root is None else str(root))


class TelegramSummary:
    """Manage Telegram message state and formatting for full-push and scan-interval modes."""

    def __init__(self):
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}
        self._last_scan_interval_push_ts: Optional[float] = None

        # Tracks (symbol, root) that have had a "new-signal" block sent in the current candle interval
        self._sent_this_interval: set = set()
        
        # Track the last candle start time to detect candle boundary
        self._last_candle_start: Optional[float] = None

        # Scoring/limits/trade manager
        self._tv_rating_weight = 0.3
        self._max_open_trades = MAX_OPEN_TRADES
        self._trade_manager = None

    def set_tv_rating_weight(self, weight: float):
        """Set the TV rating weight for combined score calculations."""
        self._tv_rating_weight = max(0.0, min(1.0, weight))

    def set_trade_manager(self, trade_manager):
        """Attach trade manager (used to inspect open trades)."""
        self._trade_manager = trade_manager

    def is_first_deploy(self) -> bool:
        return self._first_deploy_push

    def check_full_push(self, now_ts: float) -> bool:
        """Return True if this should be a full push (initial deploy or any root TF candle open)."""
        is_full_push = False
        if self._first_deploy_push:
            is_full_push = True

        for rt in ROOT_TFS:
            try:
                from .scanner_core import tf_to_seconds
                tf_seconds = tf_to_seconds(rt)
                if not tf_seconds or tf_seconds <= 0:
                    continue
                candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                last = self._last_full_push_ts.get(rt)
                if last != candle_start:
                    self._last_full_push_ts[rt] = candle_start
                    is_full_push = True
            except Exception:
                continue
        return is_full_push

    def check_candle_open(self, now_ts: float) -> bool:
        """Return True if any root TF candle just opened (and not the first deploy)."""
        if self._first_deploy_push:
            return False
        for rt in ROOT_TFS:
            try:
                from .scanner_core import tf_to_seconds
                tf_seconds = tf_to_seconds(rt)
                if not tf_seconds or tf_seconds <= 0:
                    continue
                candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                last = self._last_full_push_ts.get(rt)
                if last is not None and last != candle_start:
                    return True
            except Exception:
                continue
        return False

    def mark_full_push_sent(self):
        """Mark that the initial full-push has occurred (flip the first-deploy flag)."""
        if self._first_deploy_push:
            self._first_deploy_push = False

    def _get_smallest_root_candle_start(self, now_ts: float) -> Optional[float]:
        """Get the current candle start for the smallest ROOT_TFS timeframe."""
        try:
            if not ROOT_TFS:
                return None
            from .scanner_core import tf_to_seconds
            
            smallest_candle_start = None
            for rt in ROOT_TFS:
                tf_seconds = tf_to_seconds(rt)
                if tf_seconds and tf_seconds > 0:
                    candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                    if smallest_candle_start is None or candle_start > smallest_candle_start:
                        smallest_candle_start = candle_start
            return smallest_candle_start
        except Exception:
            return None

    async def send_summary(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]] = None,
        full_push: bool = False,
        is_candle_open: bool = False
    ):
        """Main entry to send telegram summary/messages."""
        now_ts = time.time()
        now_str = time.strftime("%H:%M UTC", time.gmtime(now_ts))

        eval_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                try:
                    sym = e.get("symbol")
                    rt = e.get("root")
                    if sym is None or rt is None:
                        continue
                    eval_map[_normalize_key(sym, rt)] = e
                except Exception:
                    continue

        if full_push:
            self._sent_this_interval.clear()
            self._last_candle_start = self._get_smallest_root_candle_start(now_ts)

            # 1) Recommended
            try:
                await self._send_recommended_block(evaluated, now_str)
            except Exception:
                logger.exception("Failed to send recommended block")

            # 2) Root TF summary counts block + short list
            try:
                await self._send_root_summary_block(root_signals, now_str)
            except Exception:
                logger.exception("Failed to send root summary block")

            # 3) A→Z listing of ALL signals: one telegram block per signal
            try:
                await self._send_all_signals_one_block_each(root_signals, eval_map, now_str)
            except Exception:
                logger.exception("Failed to send per-signal blocks for full-push")

            self.mark_full_push_sent()
        else:
            current_candle_start = self._get_smallest_root_candle_start(now_ts)
            if self._last_candle_start is not None and current_candle_start != self._last_candle_start:
                logger.info("[SCAN_CANDLE_BOUNDARY] Candle transitioned, resetting interval dedupe")
                self._sent_this_interval.clear()
            self._last_candle_start = current_candle_start
            
            try:
                await self._send_scan_interval_signals(root_signals, eval_map, now_str)
            except Exception:
                logger.exception("Failed to send scan-interval signals")

    # --- Full-push helpers ---

    async def _send_recommended_block(self, evaluated: Optional[List[Dict[str, Any]]], now_str: str):
        """Send Recommended Signals block (ranked by combined score)."""
        if not evaluated:
            return

        current_open = 0
        try:
            if self._trade_manager and hasattr(self._trade_manager, "open_trades"):
                current_open = len(self._trade_manager.open_trades)
        except Exception:
            current_open = 0

        remaining = max(0, self._max_open_trades - current_open)
        accepted = [e for e in evaluated if e and e.get("accept")]

        accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
        recommended = accepted_sorted[:remaining] if remaining > 0 else []

        lines = [f"🏆 Recommended Signals for Trading – {now_str}"]
        lines.append(f"Open trades: {current_open} / {self._max_open_trades}")
        lines.append(f"Slots available: {remaining}")

        if not recommended:
            lines.append("No recommended signals to open at this time.")
        else:
            for r in recommended:
                sym = r.get("symbol", "N/A")
                rt = r.get("root", "N/A")
                price = r.get("price")
                combined = self._compute_combined_score(r)
                score = r.get("score", 0.0)
                tv_score = r.get("tv_score", 0.0)
                price_str = self._format_price(price)
                lines.append(f"  - {sym} | {rt} | {price_str} | combined={combined:.2f} (mtf={score:.2f}, tv={tv_score:+.3f})")

        try:
            await send_message("\n".join(lines))
        except Exception:
            logger.exception("send_message failed for recommended block")

    async def _send_root_summary_block(self, root_signals: List[Dict[str, Any]], now_str: str):
        """Send Root TF summary counts breakdown block alongside alphabetical list."""
        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = str(sig.get("root", "?"))
            tf_counts[rt] = tf_counts.get(rt, 0) + 1

        lines = [f"🔍 Bybit Perp Root Summary – {now_str}"]
        lines.append("Root Timeframe Counts:")

        # Show counts per configured ROOT_TFS in order with fallback count check
        for rt in ROOT_TFS:
            cnt = tf_counts.get(rt, 0)
            lines.append(f"  - {rt}: {cnt} signals")

        lines.append("")
        lines.append("Signals (short list):")

        if not root_signals:
            lines.append("  None")
        else:
            for sig in sorted(root_signals, key=lambda s: (str(s.get("symbol", "")), str(s.get("root", "")))):
                sym = sig.get("symbol", "N/A")
                rt = sig.get("root", "N/A")
                price = sig.get("price", None)
                price_str = self._format_price(price)
                lines.append(f"  - {sym} | {rt} | {price_str}")

        try:
            await send_message("\n".join(lines))
        except Exception:
            logger.exception("send_message failed for root summary block")

    async def _send_all_signals_one_block_each(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[Tuple[str, str], Dict[str, Any]],
        now_str: str
    ):
        """Send one detailed Telegram block per signal (A→Z). Prefer root_signals values, fallback to eval_map."""
        if not root_signals:
            return

        sorted_signals = sorted(root_signals, key=lambda s: (str(s.get("symbol", "")), str(s.get("root", ""))))

        for sig in sorted_signals:
            try:
                sym_raw = sig.get("symbol", None)
                rt_raw = sig.get("root", None)
                sym, rt = _normalize_key(sym_raw, rt_raw)

                eval_entry = eval_map.get((sym, rt), {})

                price = sig.get("price", None)
                if price is None:
                    price = eval_entry.get("price")
                vol_change = sig.get("vol_change", None)
                if vol_change is None:
                    vol_change = eval_entry.get("vol_change")
                tv_label = sig.get("tv_label", None) or eval_entry.get("tv_label", "Neutral")
                tv_score = sig.get("tv_score", None)
                if tv_score is None:
                    tv_score = eval_entry.get("tv_score", 0.0)
                
                mtf_status = sig.get("mtf_status", None) or eval_entry.get("mtf_status", None)
                if not mtf_status or mtf_status == "N/A":
                    mtf_status = sig.get("reason", "aligned")
                
                negative_tfs = sig.get("negative_tfs", None) or eval_entry.get("negative_tfs", []) or []

                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(map(str, negative_tfs))})"
                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                block_lines = [
                    f"📋 Bybit Perp | {rt} Signal – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                ]

                if eval_entry:
                    try:
                        combined = self._compute_combined_score(eval_entry)
                        score = eval_entry.get("score", 0.0)
                        block_lines.append(f"Combined Score: {combined:.2f} (mtf={score:.2f})")
                    except Exception:
                        pass

                block_lines.extend([
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({float(tv_score):+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ])

                await send_message("\n".join(block_lines))
                self._sent_this_interval.add((sym, rt))
                logger.info("[FULLPUSH_SIGNAL_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send per-signal full-push block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Scan-interval helpers ---

    async def _send_scan_interval_signals(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[Tuple[str, str], Dict[str, Any]],
        now_str: str
    ):
        """Send ONLY new root signals found in this scan, one message per signal (A→Z)."""
        if not root_signals:
            logger.info("[SCAN] no root signals in this scan")
            return

        new_signals = []
        for sig in root_signals:
            sym_raw = sig.get("symbol", None)
            rt_raw = sig.get("root", None)
            sym, rt = _normalize_key(sym_raw, rt_raw)
            key = (sym, rt)
            if key not in self._sent_this_interval:
                new_signals.append((key, sig))
                logger.info("[TELEGRAM_DELTA] queued new signal: %s %s", sym, rt)

        if not new_signals:
            logger.info("[TELEGRAM_DELTA] no new signals to send this interval")
            return

        new_signals_sorted = sorted(new_signals, key=lambda t: (t[0][0], t[0][1]))

        for (sym, rt), sig in new_signals_sorted:
            try:
                eval_entry = eval_map.get((sym, rt), {})

                price = sig.get("price", None)
                if price is None:
                    price = eval_entry.get("price")
                vol_change = sig.get("vol_change", None)
                if vol_change is None:
                    vol_change = eval_entry.get("vol_change")
                tv_label = sig.get("tv_label", None) or eval_entry.get("tv_label", "Neutral")
                tv_score = sig.get("tv_score", None)
                if tv_score is None:
                    tv_score = eval_entry.get("tv_score", 0.0)
                
                mtf_status = sig.get("mtf_status", None) or eval_entry.get("mtf_status", None)
                if not mtf_status or mtf_status == "N/A":
                    mtf_status = sig.get("reason", "aligned")
                
                negative_tfs = sig.get("negative_tfs", None) or eval_entry.get("negative_tfs", []) or []

                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(map(str, negative_tfs))})"
                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                block_lines = [
                    f"📌 Bybit Perp | {rt} Signal – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                ]

                if eval_entry:
                    try:
                        combined = self._compute_combined_score(eval_entry)
                        score = eval_entry.get("score", 0.0)
                        block_lines.append(f"Combined Score: {combined:.2f} (mtf={score:.2f})")
                    except Exception:
                        pass

                block_lines.extend([
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({float(tv_score):+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ])

                block = "\n".join(block_lines)
                await send_message(block)

                self._sent_this_interval.add((sym, rt))
                logger.info("[SCAN_NEW_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send scan-interval block for %s %s", sym.get("symbol"), sym.get("root"))

    async def send_single_signal_block(self, sig: Dict[str, Any]):
        """Format and send a single signal block immediately (one message per signal)."""
        try:
            sym_raw = sig.get("symbol", None)
            rt_raw = sig.get("root", None)
            sym, rt = _normalize_key(sym_raw, rt_raw)
            key = (sym, rt)

            if key in self._sent_this_interval:
                return

            now_ts = time.time()
            now_str = time.strftime("%H:%M UTC", time.gmtime(now_ts))

            price = sig.get("price", None)
            vol_change = sig.get("vol_change", None)
            tv_label = sig.get("tv_label", "Neutral")
            tv_score = sig.get("tv_score", 0.0)
            
            mtf_status = sig.get("mtf_status", None)
            if not mtf_status or mtf_status == "N/A":
                mtf_status = sig.get("reason", "aligned")
                
            negative_tfs = sig.get("negative_tfs", []) or []

            mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(map(str, negative_tfs))})"
            price_str = self._format_price(price)
            vol_str = self._format_volume_change(vol_change)

            block_lines = [
                f"📌 Bybit Perp | {rt} Signal – {now_str}",
                f"Symbol: {sym}",
                f"Price: {price_str}",
            ]

            try:
                combined = self._compute_combined_score(sig)
                score = sig.get("score", 0.0)
                if score > 0 or combined > 0:
                    block_lines.append(f"Combined Score: {combined:.2f} (mtf={score:.2f})")
            except Exception:
                pass

            block_lines.extend([
                f"MTF Status: {mtf_str}",
                f"TV Rating: {tv_label} ({float(tv_score):+.3f})",
                f"24h Vol Δ: {vol_str}",
            ])

            block = "\n".join(block_lines)
            await send_message(block)

            self._sent_this_interval.add(key)
            logger.info("[SINGLE_SIGNAL_BLOCK_SENT] %s %s", sym, rt)
        except Exception:
            logger.exception("Failed to send single signal block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Formatting helpers ---

    def _format_price(self, price: Optional[Any]) -> str:
        """Format price with sensible precision and handle None or string input."""
        try:
            if price is None:
                return "N/A"
            if isinstance(price, str):
                try:
                    price_val = float(price)
                except Exception:
                    return price
            else:
                price_val = float(price)
            if price_val >= 1000:
                return f"${price_val:,.2f}"
            elif price_val >= 1:
                return f"${price_val:.4f}"
            else:
                return f"${price_val:.8f}"
        except Exception:
            return str(price)

    def _format_volume_change(self, vol_change: Optional[Any]) -> str:
        """Format volume change percentage (input expected as fractional e.g. 0.012 -> +1.2%)."""
        if vol_change is None:
            return "N/A"
        try:
            if isinstance(vol_change, str):
                vol_val = float(vol_change)
            else:
                vol_val = float(vol_change)
            return f"{vol_val * 100:+.1f}%"
        except Exception:
            return str(vol_change)

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        """Compute combined score from MTF (score) + TV rating (tv_score) using tv weight."""
        try:
            mtf_score = float(candidate.get("score", 0.0))
        except Exception:
            mtf_score = 0.0
        try:
            tv_score = float(candidate.get("tv_score", 0.0))
        except Exception:
            tv_score = 0.0
        combined = (mtf_score * (1.0 - self._tv_rating_weight)) + (tv_score * self._tv_rating_weight)
        return combined
