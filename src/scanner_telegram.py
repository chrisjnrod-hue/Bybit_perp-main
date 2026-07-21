# scanner_telegram_Version2.py
# Telegram messaging, state management, and message formatting.
# Implements the exact ordering and behavior you requested:
# - Full-push (initial deploy or root-TF candle open):
#     1) Recommended signals block
#     2) Root TF summary/counts block (single block)
#     3) A→Z listing of ALL signals — ONE TELEGRAM BLOCK PER SIGNAL
#     After full-push the (symbol,root) pairs are marked as "new-signal sent"
# - Scan-interval (midcandle, full_push=False):
#     - Send ONLY NEW root signals discovered in this scan, one block per signal (A→Z)
# - Dedup:
#     - _sent_this_interval avoids re-sending identical "new-signal" blocks in the same candle.
#     - Full-push clears _sent_this_interval before broadcasting so full-push always publishes fresh.

import time
from typing import Dict, List, Any, Optional
from .logger import get_logger
from .config import ROOT_TFS, MAX_OPEN_TRADES
from .telegram import send_message

logger = get_logger("scanner_telegram_version2")


class TelegramSummary:
    """Manage Telegram message state and formatting for full-push and scan-interval modes.

    Behavior:
      - Full-push: recommended -> root summary -> one block per signal (A→Z). Clears dedupe set
        before sending and marks each (symbol,root) as sent after sending the per-signal blocks.
      - Scan-interval: sends only new root signals found in the scan (one block per signal).
        Uses _sent_this_interval to avoid duplicates in the same candle.
      - Per-signal block prefers values from root_signals and falls back to evaluated entries
        when fields are missing.
    """

    def __init__(self):
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}
        self._last_scan_interval_push_ts: Optional[float] = None

        # Tracks (symbol, root) that have had a "new-signal" block sent in the current candle interval
        self._sent_this_interval: set = set()

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
                    # update last seen for this root TF and signal full-push
                    self._last_full_push_ts[rt] = candle_start
                    return True
            except Exception:
                # If tf_to_seconds fails for a TF, ignore it
                continue
        return False

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
                # candle opened if last exists and is different from current start
                if last is not None and last != candle_start:
                    return True
            except Exception:
                continue
        return False

    def mark_full_push_sent(self):
        """Mark that the initial full-push has occurred (flip the first-deploy flag)."""
        if self._first_deploy_push:
            self._first_deploy_push = False

    async def send_summary(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]] = None,
        full_push: bool = False,
        is_candle_open: bool = False
    ):
        """Main entry to send telegram summary/messages.

        Args:
          root_signals: list of root-TF signal dicts (each should include symbol, root, price, vol_change, tv_score/label, mtf_status, etc.)
          evaluated: optional list with evaluated metadata (accept, score, tv_score, tv_label, mtf_status, negative_tfs, etc.)
          full_push: True for initial deploy or candle-open full push
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())

        # Build evaluated lookup for fallback values and combined scoring
        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                try:
                    eval_map[(e["symbol"], e["root"])] = e
                except Exception:
                    continue

        if full_push:
            # For full-push we clear the per-interval dedupe so the full push is always fresh
            self._sent_this_interval.clear()

            # 1) Recommended
            try:
                await self._send_recommended_block(evaluated, now_str)
            except Exception:
                logger.exception("Failed to send recommended block")

            # 2) Root TF summary/count block (single block)
            try:
                await self._send_root_summary_block(root_signals, now_str)
            except Exception:
                logger.exception("Failed to send root summary block")

            # 3) A→Z listing of ALL signals: one telegram block per signal (detailed)
            try:
                await self._send_all_signals_one_block_each(root_signals, eval_map, now_str)
            except Exception:
                logger.exception("Failed to send per-signal blocks for full-push")

            # mark first deploy done if applicable
            self.mark_full_push_sent()
        else:
            # Scan-interval mode: only send NEW root signals, one block per new signal
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

        # sort by combined score (descending)
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
        """Send a single Root TF summary/counts block plus a short alphabetical list."""
        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = sig.get("root", "?")
            tf_counts[rt] = tf_counts.get(rt, 0) + 1

        window_map = {"60": 30, "240": 12, "D": 5}
        lines = [f"🔍 Bybit Perp Root Summary – {now_str}"]

        # show counts per configured ROOT_TFS in order
        for rt in ROOT_TFS:
            cnt = tf_counts.get(rt, 0)
            win = window_map.get(rt)
            if cnt:
                if win:
                    lines.append(f"  {rt}: {cnt} (window: {win})")
                else:
                    lines.append(f"  {rt}: {cnt}")

        lines.append("")
        lines.append("Signals (short list):")

        if not root_signals:
            lines.append("  None")
        else:
            for sig in sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", ""))):
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
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send one detailed Telegram block per signal (A→Z). Prefer root_signals values, fallback to eval_map."""
        if not root_signals:
            return

        sorted_signals = sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))

        for sig in sorted_signals:
            try:
                sym = sig.get("symbol", "N/A")
                rt = sig.get("root", "N/A")

                # prefer values from root_signals; fallback to evaluated
                eval_entry = eval_map.get((sym, rt), {})

                price = sig.get("price", eval_entry.get("price"))
                vol_change = sig.get("vol_change", eval_entry.get("vol_change"))
                tv_label = sig.get("tv_label", eval_entry.get("tv_label", "Neutral"))
                tv_score = sig.get("tv_score", eval_entry.get("tv_score", 0.0))
                mtf_status = sig.get("mtf_status", eval_entry.get("mtf_status", "N/A"))
                negative_tfs = sig.get("negative_tfs", eval_entry.get("negative_tfs", [])) or []

                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"
                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                # Build block (matches screenshot template fields)
                block_lines = [
                    f"📋 Bybit Perp | {rt} Signal – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                ]

                # If we have evaluated scoring info, include Combined Score
                if eval_entry:
                    try:
                        combined = self._compute_combined_score(eval_entry)
                        score = eval_entry.get("score", 0.0)
                        block_lines.append(f"Combined Score: {combined:.2f} (mtf={score:.2f})")
                    except Exception:
                        # ignore combined if compute fails
                        pass

                block_lines.extend([
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({tv_score:+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ])

                await send_message("\n".join(block_lines))

                # mark as sent for this interval (prevents re-sending as new-signal in same candle)
                self._sent_this_interval.add((sym, rt))
                logger.info("[FULLPUSH_SIGNAL_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send per-signal full-push block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Scan-interval helpers ---

    async def _send_scan_interval_signals(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send ONLY new root signals found in this scan, one message per signal (A→Z)."""
        if not root_signals:
            logger.info("[SCAN] no root signals in this scan")
            return

        # Find new signals (not yet sent this interval)
        new_signals = []
        for sig in root_signals:
            sym = sig.get("symbol")
            rt = sig.get("root")
            key = (sym, rt)
            if key not in self._sent_this_interval:
                new_signals.append(sig)
                logger.info("[TELEGRAM_DELTA] queued new signal: %s %s", sym, rt)

        if not new_signals:
            logger.info("[TELEGRAM_DELTA] no new signals to send this interval")
            return

        # Deterministic A→Z ordering
        new_signals_sorted = sorted(new_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))

        for sig in new_signals_sorted:
            try:
                sym = sig.get("symbol", "N/A")
                rt = sig.get("root", "N/A")
                eval_entry = eval_map.get((sym, rt), {})

                # prefer root_signals values, fallback to eval_entry
                price = sig.get("price", eval_entry.get("price"))
                vol_change = sig.get("vol_change", eval_entry.get("vol_change"))
                tv_label = sig.get("tv_label", eval_entry.get("tv_label", "Neutral"))
                tv_score = sig.get("tv_score", eval_entry.get("tv_score", 0.0))
                mtf_status = sig.get("mtf_status", eval_entry.get("mtf_status", "N/A"))
                negative_tfs = sig.get("negative_tfs", eval_entry.get("negative_tfs", [])) or []

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

                # Mark as sent in this interval so we won't re-send it in the same candle
                self._sent_this_interval.add((sym, rt))
                logger.info("[SCAN_NEW_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send scan-interval block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Formatting helpers ---

    def _format_price(self, price: Optional[float]) -> str:
        """Format price with sensible precision and handle None."""
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
        """Format volume change percentage (input expected as fractional e.g. 0.012 -> +1.2%)."""
        if vol_change is None:
            return "N/A"
        try:
            return f"{vol_change * 100:+.1f}%"
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
