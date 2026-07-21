# scanner_telegram.py
# Telegram messaging, state management, and message formatting.
# Handles dedup, push gating, and three distinct message modes.
# Behavior implemented to match screenshot templates and your spec:
# - Full-push (initial deploy or root-TF candle open):
#     1) Recommended signals block
#     2) Root TF summary/counts block (single block)
#     3) A→Z listing of ALL signals: ONE TELEGRAM BLOCK PER SIGNAL
#     After full-push the (symbol,root) pairs are marked as "new-signal sent"
# - Scan-interval (midcandle, full_push=False):
#     - Send ALL NEW root signals discovered in this scan, one block per signal (A→Z).
#     - Also send newly-resolved/MTF-aligned alerts (accept OR mtf_status == "aligned")
#       as separate one-block alerts even when the symbol/root was already sent earlier.
# - Dedup:
#     - _sent_this_interval avoids re-sending identical "new-signal" blocks in the same candle.
#     - _sent_mtf_aligned avoids re-sending identical resolved/aligned blocks.
#     - Full-push clears both sets before broadcasting (so full-push always publishes fresh).
import time
from typing import Dict, List, Any, Optional
from .logger import get_logger
from .config import ROOT_TFS, MAX_OPEN_TRADES
from .telegram import send_message

logger = get_logger("scanner_telegram")


class TelegramSummary:
    def __init__(self):
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}
        self._last_scan_interval_push_ts: Optional[float] = None

        # Tracks (symbol, root) that have had a "new-signal" block sent in the current candle interval
        self._sent_this_interval: set = set()
        # Tracks (symbol, root) that have had a "resolved/aligned" block sent (tracked separately)
        self._sent_mtf_aligned: set = set()

        self._tv_rating_weight = 0.3
        self._max_open_trades = MAX_OPEN_TRADES
        self._trade_manager = None

    def set_tv_rating_weight(self, weight: float):
        self._tv_rating_weight = max(0.0, min(1.0, weight))

    def set_trade_manager(self, trade_manager):
        self._trade_manager = trade_manager

    def is_first_deploy(self) -> bool:
        return self._first_deploy_push

    def check_full_push(self, now_ts: float) -> bool:
        if self._first_deploy_push:
            return True

        for rt in ROOT_TFS:
            try:
                from .scanner_core import tf_to_seconds
                tf_seconds = tf_to_seconds(rt)
                if not tf_seconds or tf_seconds <= 0:
                    continue
                candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                last = self._last_full_push_ts.get(rt)
                if last != candle_start:
                    # update last seen for this root TF
                    self._last_full_push_ts[rt] = candle_start
                    return True
            except Exception:
                continue
        return False

    def check_candle_open(self, now_ts: float) -> bool:
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
                if last != candle_start and last is not None:
                    return True
            except Exception:
                continue
        return False

    def mark_full_push_sent(self):
        if self._first_deploy_push:
            self._first_deploy_push = False

    async def send_summary(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]] = None,
        full_push: bool = False,
        is_candle_open: bool = False
    ):
        now_str = time.strftime("%H:%M UTC", time.gmtime())

        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        if full_push:
            # Full-push ALWAYS clears dedup state first so it publishes fresh blocks
            self._sent_this_interval.clear()
            self._sent_mtf_aligned.clear()

            await self._send_full_push(root_signals, evaluated, eval_map, now_str)
            self.mark_full_push_sent()
        else:
            await self._send_scan_interval_signals(root_signals, eval_map, now_str)

    async def _send_full_push(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]],
        ],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        # 1) Recommended block
        try:
            await self._send_recommended_block(evaluated, now_str)
        except Exception:
            logger.exception("Failed to send recommended signals block")

        # 2) Root TF summary/counts block (single block)
        try:
            await self._send_root_summary_block(root_signals, now_str)
        except Exception:
            logger.exception("Failed to send root summary block")

        # 3) A→Z listing of ALL signals: ONE TELEGRAM BLOCK PER SIGNAL
        try:
            await self._send_all_signals_one_block_each(root_signals, eval_map, now_str)
        except Exception:
            logger.exception("Failed to send per-signal detailed blocks for full-push")

    async def _send_recommended_block(self, evaluated: Optional[List[Dict[str, Any]]], now_str: str):
        if not evaluated:
            return

        current_open = len(self._trade_manager.open_trades) if self._trade_manager and hasattr(self._trade_manager, "open_trades") else 0
        remaining = max(0, self._max_open_trades - current_open)
        accepted = [e for e in evaluated if e.get("accept")]

        accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
        recommended = accepted_sorted[:remaining] if remaining > 0 else []

        rec_lines = [f"🏆 Recommended Signals for Trading – {now_str}"]
        rec_lines.append(f"Open trades: {current_open} / {self._max_open_trades}")
        rec_lines.append(f"Slots available: {remaining}")

        if not recommended:
            rec_lines.append("No recommended signals to open at this time.")
        else:
            for r in recommended:
                sym = r.get("symbol")
                rt = r.get("root")
                price = r.get("price")
                combined = self._compute_combined_score(r)
                score = r.get("score", 0.0)
                tv_score = r.get("tv_score", 0.0)
                price_str = self._format_price(price)
                rec_lines.append(f"  - {sym} | {rt} | {price_str} | combined={combined:.2f} (mtf={score:.2f}, tv={tv_score:+.3f})")

        await send_message("\n".join(rec_lines))

    async def _send_root_summary_block(self, root_signals: List[Dict[str, Any]], now_str: str):
        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = sig.get("root", "?")
            tf_counts[rt] = tf_counts.get(rt, 0) + 1

        window_map = {"60": 30, "240": 12, "D": 5}
        header_lines = [f"🔍 Bybit Perp Root Summary – {now_str}"]

        for rt in ROOT_TFS:
            cnt = tf_counts.get(rt, 0)
            win = window_map.get(rt)
            if cnt:
                if win:
                    header_lines.append(f"  {rt}: {cnt} (window: {win})")
                else:
                    header_lines.append(f"  {rt}: {cnt}")

        header_lines.append("")
        header_lines.append("Signals (short list):")

        if not root_signals:
            header_lines.append("  None")
        else:
            for sig in sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", ""))):
                sym = sig.get("symbol")
                rt = sig.get("root")
                price = sig.get("price", 0)
                price_str = self._format_price(price)
                header_lines.append(f"  - {sym} | {rt} | {price_str}")

        await send_message("\n".join(header_lines))

    async def _send_all_signals_one_block_each(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send one Telegram block per signal in alphabetical order (A→Z)."""
        if not root_signals:
            return

        sorted_signals = sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))

        for sig in sorted_signals:
            try:
                sym = sig.get("symbol")
                rt = sig.get("root")
                price = sig.get("price")
                vol_change = sig.get("vol_change")
                tv_score = sig.get("tv_score", 0.0)
                tv_label = sig.get("tv_label", "Neutral")
                eval_entry = eval_map.get((sym, rt), {})

                mtf_status = eval_entry.get("mtf_status", sig.get("mtf_status", "N/A"))
                negative_tfs = eval_entry.get("negative_tfs", sig.get("negative_tfs", [])) or []
                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"

                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                # Build per-signal block with full details (matches screenshot templates)
                block_lines = [
                    f"📋 Bybit Perp | {rt} Signal – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({tv_score:+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ]

                # If evaluated info has combined/score, show it
                if eval_entry:
                    combined = self._compute_combined_score(eval_entry)
                    score = eval_entry.get("score", 0.0)
                    block_lines.insert(3, f"Combined Score: {combined:.2f} (mtf={score:.2f})")

                await send_message("\n".join(block_lines))

                # mark as sent for this interval (new-signal)
                self._sent_this_interval.add((sym, rt))
                logger.info("[FULLPUSH_SIGNAL_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send per-signal full-push block for %s %s", sig.get("symbol"), sig.get("root"))

    async def _send_scan_interval_signals(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """
        Scan-interval behavior (midcandle):
         - Send newly-resolved/MTF-aligned alerts first (accept OR mtf_status == 'aligned'),
           one block per resolved candidate, tracked in _sent_mtf_aligned to avoid duplicates.
         - Then send ALL new root signals discovered in this scan (one block per signal),
           tracked in _sent_this_interval to avoid duplicates in the same candle.
         - Deterministic A→Z ordering within each group.
        """
        if not root_signals and not eval_map:
            return

        # Build lookup for current root signals
        sig_map: Dict[tuple, Dict[str, Any]] = {}
        for s in root_signals:
            key = (s.get("symbol"), s.get("root"))
            sig_map[key] = s

        # 1) Resolved / MTF-aligned alerts (present in evaluated and in current snapshot)
        resolved_keys = []
        for key, eval_entry in eval_map.items():
            # Only consider candidates that are present in this snapshot
            if key not in sig_map:
                continue
            sym, rt = key
            if (eval_entry.get("accept") or eval_entry.get("mtf_status") == "aligned") and key not in self._sent_mtf_aligned:
                resolved_keys.append((sym, rt, eval_entry))

        # Sort deterministically A→Z
        resolved_keys.sort(key=lambda t: (t[0] or "", t[1] or ""))

        for sym, rt, eval_entry in resolved_keys:
            try:
                sig = sig_map.get((sym, rt), {})
                price = sig.get("price")
                vol_change = sig.get("vol_change")
                tv_score = sig.get("tv_score", eval_entry.get("tv_score", 0.0))
                tv_label = sig.get("tv_label", eval_entry.get("tv_label", "Neutral"))
                mtf_status = eval_entry.get("mtf_status", "N/A")
                negative_tfs = eval_entry.get("negative_tfs", []) or []
                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"
                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)
                combined = self._compute_combined_score(eval_entry)

                reason = "accepted" if eval_entry.get("accept") else "aligned"

                lines = [
                    f"🔔 Resolved / MTF Alert – {rt} – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                    f"Combined Score: {combined:.2f}",
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({tv_score:+.3f})",
                    f"24h Vol Δ: {vol_str}",
                    f"Reason: {reason}"
                ]

                await send_message("\n".join(lines))

                self._sent_mtf_aligned.add((sym, rt))
                logger.info("[SCAN_RESOLVED_SENT] %s %s (%s)", sym, rt, reason)
            except Exception:
                logger.exception("Failed to send resolved/mtf-aligned block for %s %s", sym, rt)

        # 2) All NEW root signals: one block per new signal (A→Z)
        new_signals = []
        for sig in root_signals:
            sym = sig.get("symbol")
            rt = sig.get("root")
            key = (sym, rt)
            if key not in self._sent_this_interval:
                new_signals.append(sig)
                logger.info("[TELEGRAM_DELTA] NEW signal queued: %s %s", sym, rt)

        if not new_signals:
            logger.info("[TELEGRAM_DELTA] No new root signals to send in this interval")
            return

        new_signals_sorted = sorted(new_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))

        for sig in new_signals_sorted:
            try:
                sym = sig.get("symbol")
                rt = sig.get("root")
                price = sig.get("price")
                vol_change = sig.get("vol_change")
                tv_score = sig.get("tv_score", 0.0)
                tv_label = sig.get("tv_label", "Neutral")
                eval_entry = eval_map.get((sym, rt), {})

                mtf_status = eval_entry.get("mtf_status", sig.get("mtf_status", "N/A"))
                negative_tfs = eval_entry.get("negative_tfs", sig.get("negative_tfs", [])) or []
                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"

                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                block = "\n".join([
                    f"📌 Bybit Perp | {rt} Signal – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({tv_score:+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ])

                await send_message(block)

                # Mark as sent in this interval (prevents duplicate "new signal" blocks)
                self._sent_this_interval.add((sym, rt))
                logger.info("[SCAN_NEW_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send scan-interval signal block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Formatting helpers ---
    def _format_price(self, price: Optional[float]) -> str:
        try:
            if price is None:
                return "N/A"
            if price >= 1000:
                return f"${price:,.2f}"
            elif price >= 1:
                return f"${price:.4f}"
            else:
                return f"${price:.8f}"
        except Exception:
            return str(price)

    def _format_volume_change(self, vol_change: Optional[float]) -> str:
        if vol_change is None:
            return "N/A"
        try:
            return f"{vol_change * 100:+.1f}%"
        except Exception:
            return str(vol_change)

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - self._tv_rating_weight)) + (tv_score * self._tv_rating_weight)
        return combined
