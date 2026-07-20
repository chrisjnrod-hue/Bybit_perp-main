# scanner_telegram.py
# Telegram messaging, state management, and message formatting.
# Handles dedup, push gating, and three distinct message modes.
# FIXED: TV rating enforcement, message order, intelligent caching

import time
from typing import Dict, List, Any, Optional, Set
from .logger import get_logger
from .config import ROOT_TFS, MAX_OPEN_TRADES
from .telegram import send_message

logger = get_logger("scanner_telegram")


class TelegramSummary:
    """Manages Telegram message state, dedup, and formatting across three modes:
    
    1. INITIAL DEPLOY (full_push=True, first_deploy):
       - Recommended trading signals block per ROOT TF
       - Root TF summary with signal counts
       - Aggregated detailed signals (a-z per block)
    
    2. CANDLE OPEN (full_push=True, is_candle_open=True):
       - Recommended signals for opened ROOT TF(s)
       - Root TF summary with signal counts
       - Aggregated detailed signals (a-z per block)
    
    3. SCAN INTERVAL (full_push=False):
       - One new signal per block (deduplicated per scan interval)
    """
    
    def __init__(self):
        self._first_deploy_push = True
        self._last_full_push_ts: Dict[str, int] = {}  # Track last root TF candle start
        self._last_scan_interval_push_ts: Optional[float] = None
        self._sent_this_interval: Set[tuple] = set()  # Track (symbol, root) sent in current interval
        self._tv_rating_weight = 0.3  # Will be injected by scanner if needed
        self._max_open_trades = MAX_OPEN_TRADES
        self._trade_manager = None  # Will be set by scanner
        self._active_root_tfs: Set[str] = set()  # Track which root TFs have opened candles
        logger.info("[TELEGRAM_INIT] TelegramSummary initialized")

    def set_tv_rating_weight(self, weight: float):
        """Set the TV rating weight for combined score calculations."""
        self._tv_rating_weight = max(0.0, min(1.0, weight))

    def set_trade_manager(self, trade_manager):
        """Set reference to trade manager for open trade count."""
        self._trade_manager = trade_manager

    def is_first_deploy(self) -> bool:
        """Check if this is the first deployment push."""
        return self._first_deploy_push

    def check_full_push(self, now_ts: float) -> tuple:
        """Determine if this is a full push (initial deploy or candle open).
        
        Returns: (is_full_push: bool, opened_roots: List[str], all_active_roots: List[str])
        """
        opened_roots = []
        
        if self._first_deploy_push:
            # Initial deploy: all ROOT_TFS are "active" for this cycle
            self._active_root_tfs = set(ROOT_TFS)
            for rt in ROOT_TFS:
                try:
                    from .scanner_core import tf_to_seconds
                    tf_seconds = tf_to_seconds(rt)
                    if tf_seconds and tf_seconds > 0:
                        candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                        self._last_full_push_ts[rt] = candle_start
                except Exception:
                    pass
            logger.info("[TELEGRAM_CANDLE] FIRST DEPLOY - all roots active: %s", list(self._active_root_tfs))
            return True, list(ROOT_TFS), list(ROOT_TFS)
        
        # Check if any root TF candle has opened (new candle start)
        for rt in ROOT_TFS:
            try:
                from .scanner_core import tf_to_seconds
                tf_seconds = tf_to_seconds(rt)
                if not tf_seconds or tf_seconds <= 0:
                    continue
                candle_start = (int(now_ts) // tf_seconds) * tf_seconds
                last = self._last_full_push_ts.get(rt)
                
                if last != candle_start and last is not None:
                    # Candle opened for this root TF
                    opened_roots.append(rt)
                    self._active_root_tfs.add(rt)
                    self._last_full_push_ts[rt] = candle_start
                    logger.info("[TELEGRAM_CANDLE] CANDLE OPENED for %s (last=%s, now=%s)", rt, last, candle_start)
                elif last is None:
                    # First time tracking this TF
                    self._last_full_push_ts[rt] = candle_start
                    self._active_root_tfs.add(rt)
                    logger.info("[TELEGRAM_CANDLE] First tracking %s at %s", rt, candle_start)
            except Exception as e:
                logger.exception("[TELEGRAM_CANDLE] Error checking candle open for %s: %s", rt, e)
                continue
        
        is_full_push = len(opened_roots) > 0
        all_active_roots = sorted(self._active_root_tfs, key=lambda x: ROOT_TFS.index(x) if x in ROOT_TFS else 999)
        
        logger.info("[TELEGRAM_CANDLE] is_full_push=%s opened_roots=%s active_roots=%s", is_full_push, opened_roots, all_active_roots)
        return is_full_push, opened_roots, all_active_roots

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
        """Mark full push as sent and reset interval dedup."""
        self._sent_this_interval.clear()
        if self._first_deploy_push:
            self._first_deploy_push = False
            logger.info("[TELEGRAM_STATE] First deploy marked complete, now in normal mode")

    async def send_summary(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]] = None,
        full_push: bool = False,
        is_candle_open: bool = False,
        opened_roots: Optional[List[str]] = None,
        all_active_roots: Optional[List[str]] = None
    ):
        """Send Telegram summary in one of three modes.
        
        Args:
            root_signals: List of root TF signals detected
            evaluated: List of evaluated candidates with MTF alignment
            full_push: True for initial deploy or candle opens
            is_candle_open: True if a root TF candle just opened
            opened_roots: List of ROOT_TFs that just opened candles
            all_active_roots: List of all ROOT_TFs that are "active" this cycle
        """
        now_str = time.strftime("%H:%M UTC", time.gmtime())
        
        eval_map: Dict[tuple, Dict[str, Any]] = {}
        if evaluated:
            for e in evaluated:
                eval_map[(e["symbol"], e["root"])] = e
        
        if full_push:
            # FULL PUSH MODE: Initial deploy or candle open
            logger.info("[TELEGRAM_SEND_FULL] full_push=True, opened_roots=%s, all_active_roots=%s", opened_roots, all_active_roots)
            await self._send_full_push(
                root_signals,
                evaluated,
                eval_map,
                now_str,
                opened_roots or [],
                all_active_roots or []
            )
        else:
            # SCAN INTERVAL MODE: Send one new signal per block
            logger.info("[TELEGRAM_SEND_INTERVAL] full_push=False, sending delta signals")
            await self._send_scan_interval_signals(root_signals, eval_map, now_str)

    async def _send_full_push(
        self,
        root_signals: List[Dict[str, Any]],
        evaluated: Optional[List[Dict[str, Any]]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str,
        opened_roots: List[str],
        all_active_roots: List[str]
    ):
        """Send full push with recommended blocks per opened root, then unified summary."""
        logger.info("[TELEGRAM_FULL_PUSH_START] all_active_roots=%s opened_roots=%s signals_count=%d", 
                    all_active_roots, opened_roots, len(root_signals))
        
        # Filter signals to only those for active roots (for this push cycle)
        active_signals = [s for s in root_signals if s.get("root") in all_active_roots]
        logger.info("[TELEGRAM_FULL_PUSH_FILTER] filtered signals: %d total, %d for active roots", 
                    len(root_signals), len(active_signals))
        
        # --- 1) RECOMMENDED BLOCK PER OPENED ROOT ---
        # Only show recommendations for newly-opened roots
        for rt in opened_roots:
            try:
                rt_evaluated = [e for e in (evaluated or []) if e.get("root") == rt]
                await self._send_recommended_block_for_root(rt_evaluated, rt, now_str)
            except Exception:
                logger.exception("[TELEGRAM_FULL_PUSH] Failed to send recommended block for root %s", rt)
        
        # --- 2) ROOT TF SUMMARY HEADER (counts for all active roots) ---
        try:
            await self._send_root_summary_block(active_signals, all_active_roots, now_str)
        except Exception:
            logger.exception("[TELEGRAM_FULL_PUSH] Failed to send root summary block")
        
        # --- 3) AGGREGATED ALPHABETICAL DETAILED SIGNALS (ONE BLOCK) ---
        try:
            await self._send_detailed_signals_block(active_signals, eval_map, now_str)
        except Exception:
            logger.exception("[TELEGRAM_FULL_PUSH] Failed to send aggregated detailed signals block")
        
        logger.info("[TELEGRAM_FULL_PUSH_COMPLETE]")

    async def _send_recommended_block_for_root(
        self,
        rt_evaluated: List[Dict[str, Any]],
        root: str,
        now_str: str
    ):
        """Send recommended signals block for a specific root TF."""
        current_open = len(self._trade_manager.open_trades) if self._trade_manager and hasattr(self._trade_manager, "open_trades") else 0
        remaining = max(0, self._max_open_trades - current_open)
        accepted = [e for e in rt_evaluated if e.get("accept")]
        
        # SORT BY COMBINED SCORE (descending)
        accepted_sorted = sorted(accepted, key=lambda r: self._compute_combined_score(r), reverse=True)
        recommended = accepted_sorted[:remaining] if remaining > 0 else []
        
        rec_lines = [f"🏆 Recommended Signals – {root} TF – {now_str}"]
        rec_lines.append(f"Open trades: {current_open} / {self._max_open_trades}")
        rec_lines.append(f"Slots available: {remaining}")
        
        if not recommended:
            rec_lines.append("No recommended signals to open at this time.")
        else:
            for r in recommended:
                sym = r["symbol"]
                price = r["price"]
                combined = self._compute_combined_score(r)
                score = r.get("score", 0.0)
                tv_score = r.get("tv_score", 0.0)
                price_str = self._format_price(price)
                rec_lines.append(f"  - {sym} | {price_str} | combined={combined:.2f} (mtf={score:.2f}, tv={tv_score:+.3f})")
        
        logger.info("[TELEGRAM_RECOMMENDED] Sending recommended block for %s with %d signals", root, len(recommended))
        await send_message("\n".join(rec_lines))

    async def _send_root_summary_block(
        self,
        root_signals: List[Dict[str, Any]],
        all_active_roots: List[str],
        now_str: str
    ):
        """Send root TF summary header with signal counts for active roots."""
        tf_counts: Dict[str, int] = {}
        for sig in root_signals:
            rt = sig.get("root", "?")
            tf_counts[rt] = tf_counts.get(rt, 0) + 1
        
        window_map = {"60": 30, "240": 12, "D": 5, "1h": 30, "4h": 12, "1d": 5}
        header_lines = [f"🔍 Bybit Perp Root Summary – {now_str}"]
        
        for rt in all_active_roots:
            cnt = tf_counts.get(rt, 0)
            win = window_map.get(rt)
            if cnt > 0:
                if win:
                    header_lines.append(f"  {rt}: {cnt} (window: {win})")
                else:
                    header_lines.append(f"  {rt}: {cnt}")
            else:
                header_lines.append(f"  {rt}: 0")
        
        header_lines.append("")
        header_lines.append("All Signals:")
        
        if not root_signals:
            header_lines.append("  None")
        else:
            for sig in sorted(root_signals, key=lambda s: (s.get("symbol", ""), s.get("root", ""))):
                sym = sig.get("symbol")
                rt = sig.get("root")
                price = sig.get("price", 0)
                price_str = self._format_price(price)
                header_lines.append(f"  - {sym} | {rt} | {price_str}")
        
        logger.info("[TELEGRAM_SUMMARY] Sending root summary with %d signals", len(root_signals))
        await send_message("\n".join(header_lines))

    async def _send_detailed_signals_block(
        self,
        root_signals: List[Dict[str, Any]],
        eval_map: Dict[tuple, Dict[str, Any]],
        now_str: str
    ):
        """Send aggregated detailed signals (one block, a-z by symbol then by root)."""
        if not root_signals:
            logger.info("[TELEGRAM_DETAILS] No signals to send in detail block")
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
        
        logger.info("[TELEGRAM_DETAILS] Sending detail block with %d signals", len(sorted_signals))
        await send_message("\n".join(detail_lines))
        
        # Mark all signals as sent in this full push
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
        """Send one new signal per block (scan interval mode with dedup)."""
        if not root_signals:
            logger.info("[TELEGRAM_INTERVAL] No root signals this interval")
            return
        
        # Find new signals (not yet sent in this interval)
        to_send = []
        for sig in root_signals:
            sym = sig.get("symbol")
            rt = sig.get("root")
            key = (sym, rt)
            
            if key not in self._sent_this_interval:
                to_send.append(sig)
                logger.info("[TELEGRAM_INTERVAL] NEW signal queued: %s %s", sym, rt)
        
        if not to_send:
            logger.info("[TELEGRAM_INTERVAL] No new signals to send (all deduplicated)")
            return
        
        logger.info("[TELEGRAM_INTERVAL] Sending %d new signals", len(to_send))
        
        # Send one Telegram message per new signal
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
                
                # Mark as sent in current interval
                self._sent_this_interval.add((sym, rt))
                logger.info("[TELEGRAM_INTERVAL_SENT] %s %s", sym, rt)
            except Exception:
                logger.exception("[TELEGRAM_INTERVAL] Failed to send signal block for %s %s", sig.get("symbol"), sig.get("root"))

    # --- Formatting helpers ---

    def _format_price(self, price: float) -> str:
        """Format price for display based on magnitude."""
        try:
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
