# scanner_telegram.py
# Handles formatting and sequential dispatch of Telegram notifications.

import time
import logging
from typing import List, Dict, Any
from .telegram import send_message

logger = logging.getLogger("scanner_telegram")

class TelegramSummary:
    def __init__(self):
        self._first_deploy = True
        self._last_full_push = 0.0

    def is_first_deploy(self) -> bool:
        return self._first_deploy

    def check_full_push(self, now_ts: float) -> bool:
        """Returns True on the very first deploy scan."""
        if self._first_deploy:
            return True
        return False

    def check_candle_open(self, now_ts: float) -> bool:
        """Determines if this run is near the top of an hour."""
        current_minute = time.gmtime(now_ts).tm_min
        return current_minute < 5

    def mark_full_push_sent(self):
        self._first_deploy = False
        self._last_full_push = time.time()

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: List[Dict[str, Any]], full_push: bool, is_candle_open: bool, new_candle_tfs: List[str] = None):
        if new_candle_tfs is None:
            new_candle_tfs = []
        
        # We only send messages on the initial deploy OR when a new root candle opens
        if not full_push and not new_candle_tfs:
            return

        # Separate signals by block/root
        blocks = {"60": [], "240": [], "D": []}
        all_signals_az = []
        recommended = {"60": [], "240": [], "D": []}
        
        for sig in evaluated:
            root = sig.get("root")
            sym = sig.get("symbol")
            
            # Map root to our standard keys
            if root in ["60", "1h", "1H"]: r_key = "60"
            elif root in ["240", "4h", "4H"]: r_key = "240"
            elif root in ["D", "1d", "1D"]: r_key = "D"
            else: continue

            # Sort into blocks
            if r_key in blocks:
                blocks[r_key].append(sym)
                all_signals_az.append(sym)
            
            # Sort into recommended if accepted
            if sig.get("accept"):
                recommended[r_key].append(sym)

        # Ensure alphabetical sorting (A-Z)
        all_signals_az = sorted(list(set(all_signals_az)))
        for r in blocks:
            blocks[r] = sorted(list(set(blocks[r])))
            recommended[r] = sorted(list(set(recommended[r])))

        # Optional: Announce the scan trigger
        if full_push:
            await send_message("🚀 **Initial Deploy Scan**")
        elif new_candle_tfs:
            labels = []
            if "60" in new_candle_tfs: labels.append("1H")
            if "240" in new_candle_tfs: labels.append("4H")
            if "D" in new_candle_tfs: labels.append("1D")
            await send_message(f"⏱ **New Candle Scan:** {', '.join(labels)}")

        # 1. SEND ONCE: Recommended Signals Per Block
        rec_msg = "🔥 **Recommended Signals**\n"
        has_rec = False
        for tf, label in [("60", "1H"), ("240", "4H"), ("D", "1D")]:
            # Only include blocks that opened (or all if deploy)
            if full_push or tf in new_candle_tfs:
                if recommended[tf]:
                    rec_msg += f"\n**{label} Block:**\n" + ", ".join(recommended[tf]) + "\n"
                    has_rec = True
                else:
                    rec_msg += f"\n**{label} Block:**\nNone\n"
                    has_rec = True
        
        if has_rec:
            await send_message(rec_msg.strip())

        # 2. SEND ONCE: Signal Summary A-Z (All combined)
        if all_signals_az:
            az_msg = "📊 **Signal Summary (A-Z)**\n" + ", ".join(all_signals_az)
            await send_message(az_msg.strip())

        # 3. SEND ONCE: Complete Signals A-Z Per Block
        block_msg = "🗂 **Signals Per Block (Complete)**\n"
        has_blocks = False
        for tf, label in [("60", "1H"), ("240", "4H"), ("D", "1D")]:
            if full_push or tf in new_candle_tfs:
                if blocks[tf]:
                    block_msg += f"\n**{label} Block:**\n" + ", ".join(blocks[tf]) + "\n"
                    has_blocks = True
                else:
                    block_msg += f"\n**{label} Block:**\nNone\n"
                    has_blocks = True
                    
        if has_blocks:
            await send_message(block_msg.strip())
