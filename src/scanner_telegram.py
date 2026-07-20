# scanner_telegram.py
# Handles formatting and sequential dispatch of Telegram notifications.

import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
from .telegram import send_message

logger = logging.getLogger("scanner_telegram")

class TelegramSummary:
    def __init__(self):
        self._first_deploy = True
        self._last_full_push = 0.0
        self._sent_signals = set() # Tracks sent signals mid-candle to prevent duplication

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
        self._sent_signals.clear() # Reset duplication tracker on full push

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: List[Dict[str, Any]], full_push: bool, is_candle_open: bool, new_candle_tfs: List[str] = None):
        if new_candle_tfs is None:
            new_candle_tfs = []
            
        now_str = datetime.now(timezone.utc).strftime("%H:%M")
        
        # Standardize and map blocks
        blocks = {"60": [], "240": [], "D": []}
        recommended = {"60": [], "240": [], "D": []}
        all_signals_data = []

        for sig in evaluated:
            root = sig.get("root")
            sym = sig.get("symbol")
            
            # Map root to our standard keys
            if root in ["60", "1h", "1H"]: r_key = "60"
            elif root in ["240", "4h", "4H"]: r_key = "240"
            elif root in ["D", "1d", "1D"]: r_key = "D"
            else: continue

            # Populate dictionary with defaults for missing data
            sig_data = {
                "symbol": sym,
                "root": r_key,
                "price": sig.get("price", "0.00000000"),
                "mtf_status": sig.get("mtf_status", "monitoring"),
                "neg": sig.get("neg", "5"), 
                "tv_rating": sig.get("tv_rating", "Neutral (+0.000)"),
                "vol_delta": sig.get("vol_delta", "+0.0%"),
                "accept": sig.get("accept", False),
                "open_trades": sig.get("open_trades", 0),
                "max_trades": sig.get("max_trades", 3),
                "score": sig.get("score", "0.00"),
                "reason": sig.get("reason", "aligned")
            }
            
            blocks[r_key].append(sig_data)
            all_signals_data.append(sig_data)
            
            # Sort into recommended if accepted
            if sig_data["accept"]:
                recommended[r_key].append(sig_data)

        # Ensure alphabetical sorting (A-Z)
        all_signals_data.sort(key=lambda x: x["symbol"])
        for r in blocks:
            blocks[r].sort(key=lambda x: x["symbol"])
            recommended[r].sort(key=lambda x: x["symbol"])

        is_scan_event = full_push or new_candle_tfs

        # =======================================================
        # BLOCK 1: STARTUP / DEPLOY & CANDLE OPEN ROOT SCANS
        # =======================================================
        if is_scan_event:
            self._sent_signals.clear() # Clear tracking to allow fresh dispatch 
            
            # 1. 🔍 A-Z Signal Summary / Count Block (Ref: Screenshot_20260720-221332.jpg)
            summary_msg = f"🔍 Bybit Perp Root Summary – {now_str} UTC\n"
            summary_msg += f"60: {len(blocks['60'])} (window: 30)\n"
            summary_msg += f"240: {len(blocks['240'])} (window: 12)\n"
            summary_msg += f"D: {len(blocks['D'])} (window: 5)\n\n"
            summary_msg += "All Signals:\n"
            for s in all_signals_data:
                summary_msg += f"- {s['symbol']} | {s['root']} | ${s['price']}\n"
            
            await send_message(summary_msg.strip())

            # 2. 🏆 Recommended Signals Blocks (Ref: Screenshot_20260720-221319~2.png)
            for tf in ["60", "240", "D"]:
                if full_push or tf in new_candle_tfs:
                    # Dispatch accepted/simulated trades first
                    for rec in recommended[tf]:
                        sim_msg = (
                            f"🔔 Simulated Trade – {rec['symbol']} Buy\n"
                            f"Price: {rec['price']} | Qty: 1.000000\n"
                            f"Combined Score: {rec['score']} | TV Rating: {rec['tv_rating']} | "
                            f"Reason: {rec['reason']}"
                        )
                        await send_message(sim_msg.strip())

                    # Dispatch recommended block summary
                    open_count = recommended[tf][0]['open_trades'] if recommended[tf] else 0
                    max_count = recommended[tf][0]['max_trades'] if recommended[tf] else 3
                    slots = max(0, max_count - open_count)

                    rec_msg = f"🏆 Recommended Signals – {tf} TF – {now_str} UTC\n"
                    rec_msg += f"Open trades: {open_count} / {max_count}\n"
                    rec_msg += f"Slots available: {slots}\n"
                    if not recommended[tf]:
                        rec_msg += "No recommended signals to open at this time."
                    
                    await send_message(rec_msg.strip())

            # 3. 📌 A-Z Listed Signals Complete with Symbol Info (Ref: Screenshot_20260720-221343.jpg)
            for tf in ["60", "240", "D"]:
                if full_push or tf in new_candle_tfs:
                    for s in blocks[tf]:
                        sig_msg = (
                            f"📌 Bybit Perp | {s['root']} Signal – {now_str} UTC\n"
                            f"Symbol: {s['symbol']}\n"
                            f"Price: ${s['price']}\n"
                            f"MTF Status: {s['mtf_status']} (neg: {s['neg']})\n"
                            f"TV Rating: {s['tv_rating']}\n"
                            f"24h Vol Δ: {s['vol_delta']}"
                        )
                        await send_message(sig_msg.strip())
                        
                        # Add to sent tracker
                        self._sent_signals.add(f"{s['symbol']}_{s['root']}")
        
        # =======================================================
        # BLOCK 2: MID-CANDLE UPDATES (INTERVAL = 0)
        # =======================================================
        else:
            for s in all_signals_data:
                sig_id = f"{s['symbol']}_{s['root']}"
                
                # Check tracker: only send if the signal is distinctly new for this candle
                if sig_id not in self._sent_signals:
                    sig_msg = (
                        f"📌 Bybit Perp | {s['root']} Signal – {now_str} UTC\n"
                        f"Symbol: {s['symbol']}\n"
                        f"Price: ${s['price']}\n"
                        f"MTF Status: {s['mtf_status']} (neg: {s['neg']})\n"
                        f"TV Rating: {s['tv_rating']}\n"
                        f"24h Vol Δ: {s['vol_delta']}"
                    )
                    await send_message(sig_msg.strip())
                    
                    # If this midcandle signal is an accepted trade alignment, push simulated update
                    if s["accept"]:
                        sim_msg = (
                            f"🔔 Simulated Trade – {s['symbol']} Buy\n"
                            f"Price: {s['price']} | Qty: 1.000000\n"
                            f"Combined Score: {s['score']} | TV Rating: {s['tv_rating']} | "
                            f"Reason: {s['reason']}"
                        )
                        await send_message(sim_msg.strip())

                    # Store in tracker to prevent duplication on the next mid-candle loop
                    self._sent_signals.add(sig_id)
