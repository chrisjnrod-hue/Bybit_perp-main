# scanner_telegram.py
# Telegram messaging, state management, and message formatting.
# Handles dedup, push gating, and three distinct message modes.

import time
from typing import Dict, List, Any, Optional
from .logger import get_logger
from .config import ROOT_TFS, MAX_OPEN_TRADES
from .telegram import send_message

logger = get_logger("scanner_telegram")


class TelegramSummary:
    """Manages Telegram message state, dedup, and formatting across three modes:
    
    1. INITIAL DEPLOY (full_push=True, first_deploy):
       - Recommended trading signals block
       - Root TF summary with signal counts
       - Aggregated detailed signals (alphabetical)
    
    2. SCAN INTERVAL (full_push=False):
       - Send ALL new root signals found in this scan as separate Telegram blocks
       - Also send resolved / MTF-aligned alerts as separate Telegram blocks even
         if the symbol/root was already sent as a "new signal" previously.
       - Track resolved/aligned alerts separately to avoid duplicate aligned alerts.
    
    3. ROOT TFS CANDLE OPEN (full_push=True, is_candle_open=True):
       - Same as INITIAL DEPLOY format
    """
    
    def __init__(self):
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}  # Track last root TF candle start
        self._last_scan_interval_push_ts: Optional[float] = None
        self._sent_this_interval: set = set()  # Track (symbol, root) "new signal" blocks sent in current interval
        self._sent_mtf_aligned: set = set()  # Track (symbol, root) "resolved/aligned" blocks sent (separate)
        self._tv_rating_weight = 0.3  # Will be injected by scanner if needed
        self._max_open_trades = MAX_OPEN_TRADES
        self._trade_manager = None  # Will be set by scanner

    def set_tv_rating_weight(self, weight: float):
        """Set the TV rating weight for combined score calculations."""
        self._tv_rating_weight = max(0.0, min(1.0, weight))

    def set_trade_manager(self, trade_manager):
        """Set reference to trade manager for open trade count."""
        self._trade_manager = trade_manager

    def is_first_deploy(self) -> bool:
        """Check if this is the first deployment push."""
        return self._first_deploy_push

    def check_full_push(self, now_ts: float) -> bool:
        """Determine if this is a full push (initial deploy or candle open)."""
        if self._first_deploy_push:
            return True
        
        # Check if any root TF candle has opened (new candle start)
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
        """Check if a candle just opened (not first deploy)."""
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
        """Mark full push as completed (flip first-deploy state).

        Note: We intentionally do not clear dedup sets here. The sets are reset
        before sending a full push so the full-push will not be suppressed by
        prior state; after sending, the detailed block itself marks signals as sent.
        """
        if self._first_deploy_push:
            self._first_deploy_push = False

    async def send_summary(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]] = None,
        full_push: bool = False,
        is_candle_open: bool = False
    ):
        """Send Telegram summary in one of three modes.

        Args:
            root_signals: List of root TF signals detected
            evaluated: List of evaluated candidates with MTF alignment
            full_push: True for initial deploy or candle opens
            is_candle_open: True if a root TF candle just opened
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())
        
        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e

        if full_push:
            # For a full push (initial deploy or candle open) reset per-interval sent state
            # so the full push will publish fresh aggregated blocks. This prevents old
            # interval dedupe from blocking a legitimate full-push broadcast.
            self._sent_this_interval.clear()
            self._sent_mtf_aligned.clear()

            # FULL PUSH MODE: Initial deploy or candle open
            await self._send_full_push(root_signals, evaluated, eval_map, now_str)

            # After a proper full push, mark that first-deploy is done (if applicable)
            self.mark_full_push_sent()
        else:
            # SCAN INTERVAL MODE: Send all new root signals (one block each) and separate
            # resolved/aligned alerts (one block each) tracked separately.
            await self._send_scan_interval_signals(root_signals, eval_map, now_str)

    async def _send_full_push(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send full push with recommended block, summary header, and aggregated details."""
        
        # --- 1) Recommended block (sent first) ---
        try:
            await self._send_recommended_block(evaluated, now_str)
        except Exception:
            logger.exception("Failed to send recommended signals block")
        
        # --- 2) Root TF summary header (counts) ---
        try:
            await self._send_root_summary_block(root_signals, now_str)
        except Exception:
            logger.exception("Failed to send root summary block")
        
        # --- 3) Aggregated alphabetical detailed signals (ONE BLOCK) ---
        try:
            await self._send_detailed_signals_block(root_signals, eval_map, now_str)
        except Exception:
            logger.exception("Failed to send aggregated detailed signals block")

    async def _send_recommended_block(self, evaluated: Optional[List[Dict[str, Any]]], now_str: str):
        """Send recommended trading signals block."""
        if not evaluated:
            return
        
        current_open = len(self._trade_manager.open_trades) if self._trade_manager and hasattr(self._trade_manager, "open_trades") else 0
        remaining = max(0, self._max_open_trades - current_open)
        accepted = [e for e in evaluated if e.get("accept")]
        
        # SORT BY COMBINED SCORE (descending)
        accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
        recommended = accepted_sorted[:remaining] if remaining > 0 else []
        
        rec_lines = [f"🏆 Recommended Signals for Trading – {now_str}"]
        rec_lines.append(f"Open trades: {current_open} / {self._max_open_trades}")
        rec_lines.append(f"Slots available: {remaining}")
        
        if not recommended:
            rec_lines.append("No recommended signals to open at this time.")
        else:
            for r in recommended:
                sym = r["symbol"]
                rt = r["root"]
                price = r.get("price")
                combined = self._compute_combined_score(r)
                score = r.get("score", 0.0)
                tv_score = r.get("tv_score", 0.0)
                price_str = self._format_price(price)
                rec_lines.append(f"  - {sym} | {rt} | {price_str} | combined={combined:.2f} (mtf={score:.2f}, tv={tv_score:+.3f})")
        
        await send_message("\n".join(rec_lines))

    async def _send_root_summary_block(self, root_signals: List[Dict[str, Any]], now_str: str):
        """Send root TF summary header with signal counts."""
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
        header_lines.append("Signals (all root TF signals):")
        
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

    async def _send_detailed_signals_block(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send aggregated detailed signals (one block, alphabetical)."""
        if not root_signals:
            return
        
        sorted_signals = sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", "")))
        
        detail_lines = [f"📋 Signals (detailed) – {now_str}"]
        for sig in sorted_signals:
            sym = sig.get("symbol")
            rt = sig.get("root")
            price = sig.get("price", 0)
            vol_change = sig.get("vol_change")
            tv_score = sig.get("tv_score", 0.0)
            tv_label = sig.get("tv_label", "Neutral")
            eval_entry = eval_map.get((sym, rt), {})
            
            mtf_status = eval_entry.get("mtf_status", "N/A")
            negative_tfs = eval_entry.get("negative_tfs", [])
            mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"
            
            price_str = self._format_price(price)
            vol_str = self._format_volume_change(vol_change)
            
            detail_lines.append(f"  - {sym} | {rt} | {price_str} | TV: {tv_label} ({tv_score:+.3f}) | 24h Vol Δ: {vol_str} | MTF: {mtf_str}")
        
        await send_message("\n".join(detail_lines))
        
        # Mark all signals as sent in this full push (these are "new signal" blocks for this interval)
        for sig in root_signals:
            sym = sig.get("symbol")
            rt = sig.get("root")
            self._sent_this_interval.add((sym, rt))

    async def _send_scan_interval_signals(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send all new root signals (one block each) and any newly resolved/aligned MTF alerts.

        Behavior:
          - Sends one Telegram message per new root signal (symbol, root) that has NOT
            already been sent in this interval.
          - Sends one Telegram message per resolved/mtf-aligned candidate (tracked
            by _sent_mtf_aligned) even if that symbol was already sent as a "new signal".
          - Avoids duplicate identical blocks by tracking two separate sets:
              _sent_this_interval -> "new signal" blocks
              _sent_mtf_aligned   -> "resolved/aligned" blocks
        """
        if not root_signals:
            return

        # Build quick lookup for the current root_signals
        sig_map: Dict[tuple, Dict[str, Any]] = {}
        for s in root_signals:
            key = (s.get("symbol"), s.get("root"))
            sig_map[key] = s

        # 1) Handle resolved / MTF-aligned alerts first (so actionable alerts go out promptly).
        #    These are derived from eval_map entries (if present) and are sent even if the
        #    symbol/root was already sent as a "new signal". Each resolved alert is only
        #    sent once and tracked via _sent_mtf_aligned.
        resolved_to_send: List[tuple] = []
        for key, eval_entry in eval_map.items():
            # only consider candidates that are present in this root_signals snapshot
            if key not in sig_map:
                continue
            # resolved criteria: explicit accept OR mtf_status == 'aligned'
            if eval_entry.get("accept") or eval_entry.get("mtf_status") == "aligned":
                if key not in self._sent_mtf_aligned:
                    resolved_to_send.append((key, eval_entry))

        # Deterministic ordering
        resolved_to_send.sort(key=lambda t: (t[0][0] or "", t[0][1] or ""))

        for (sym, rt), eval_entry in resolved_to_send:
            try:
                sig = sig_map.get((sym, rt), {})
                price = sig.get("price")
                vol_change = sig.get("vol_change")
                tv_score = sig.get("tv_score", 0.0) if sig else eval_entry.get("tv_score", 0.0)
                tv_label = sig.get("tv_label", eval_entry.get("tv_label", "Neutral"))
                combined = self._compute_combined_score(eval_entry) if eval_entry else None
                mtf_status = eval_entry.get("mtf_status", "N/A")
                negative_tfs = eval_entry.get("negative_tfs", [])
                mtf_str = mtf_status if not negative_tfs else f"{mtf_status} (neg: {','.join(negative_tfs)})"
                price_str = self._format_price(price)
                vol_str = self._format_volume_change(vol_change)

                reason = "accepted" if eval_entry.get("accept") else "aligned"
                # Build resolved/aligned block
                lines = [
                    f"🔔 Resolved / MTF Alert – {rt} – {now_str}",
                    f"Symbol: {sym}",
                    f"Price: {price_str}",
                    f"MTF Status: {mtf_str}",
                    f"TV Rating: {tv_label} ({tv_score:+.3f})",
                    f"24h Vol Δ: {vol_str}",
                ]
                if combined is not None:
                    lines.insert(3, f"Combined Score: {combined:.2f}")
                lines.append(f"Reason: {reason}")

                await send_message("\n".join(lines))

                # mark this resolved alert as sent (prevents duplicate aligned alerts)
                self._sent_mtf_aligned.add((sym, rt))
                logger.info("[TELEGRAM_RESOLVED_SENT] %s %s (%s)", sym, rt, reason)
            except Exception:
                logger.exception("Failed to send resolved/mtf-aligned block for %s %s", sym, rt)

        # 2) Send all NEW root signals (one block per new signal) that haven't been sent
        #    in this interval. This sends every new root signal as requested.
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

        # deterministic alphabetical ordering
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

                mtf_status = eval_entry.get("mtf_status", "N/A")
                negative_tfs = eval_entry.get("negative_tfs", [])
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

                # Mark as sent in current interval (prevents duplicate "new signal" block)
                self._sent_this_interval.add((sym, rt))
                logger.info("[TELEGRAM_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("Failed to send scan interval signal block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Formatting helpers ---

    def _format_price(self, price: float) -> str:
        """Format price for display based on magnitude."""
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
        """Format volume change percentage."""
        if vol_change is None:
            return "N/A"
        try:
            return f"{vol_change * 100:+.1f}%"
        except Exception:
            return str(vol_change)

    def _compute_combined_score(self, candidate: Dict[str, Any]) -> float:
        """Compute combined score from MTF + TV rating."""
        mtf_score = candidate.get("score", 0.0)
        tv_score = candidate.get("tv_score", 0.0)
        combined = (mtf_score * (1.0 - self._tv_rating_weight)) + (tv_score * self._tv_rating_weight)
        return combined
