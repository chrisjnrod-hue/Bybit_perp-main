import asyncio
import time
from typing import Dict, List, Any, Optional

from .logger import get_logger
from .config import (
    ROOT_TFS, ROOT_SCAN_INTERVAL, REQUEST_BATCH_SIZE, REQUEST_BATCH_DELAY
)
from .telegram import send_message
from .scanner_base import ScannerBase

logger = get_logger("scanner_service")

class Scanner(ScannerBase):
    def __init__(self):
        super().__init__()

    async def root_scan_loop(self):
        while not self._stop:
            start = time.time()
            if not self.symbols: await self.discover_symbols()
            
            root_signals = []
            async def check_symbol(sym: str):
                price = await self.client.get_latest_price(sym)
                if not price: return
                self._last_price_cache[sym] = price
                await self._update_24h_volume(sym)
                for root in ROOT_TFS:
                    macd_line, sig, hist = self.compute_macd_for(sym, root, include_price=price, use_ws_current=True)
                    if self.detect_flip_current_open(hist, 0.0):
                        last_c = self.kline_store.get(sym, {}).get(root, [])
                        start_at = last_c[-1].get("start_at") if last_c else time.time()
                        tv_score, tv_label = self.compute_tv_rating(sym, root, price)
                        root_signals.append({
                            "symbol": sym, "root": root, "price": price, "hist": hist,
                            "vol_change": self.compute_24h_volume_change(sym),
                            "start_at": start_at, "tv_score": tv_score, "tv_label": tv_label
                        })

            for i in range(0, len(self.symbols), REQUEST_BATCH_SIZE):
                batch = self.symbols[i:i + REQUEST_BATCH_SIZE]
                await asyncio.gather(*[check_symbol(s) for s in batch])
                await asyncio.sleep(REQUEST_BATCH_DELAY)

            await self._check_monitored_symbols()
            evaluated = await self.handle_root_signals(root_signals, allow_open_trades=True)
            await self.send_summary(root_signals, evaluated=evaluated, full_push=True)
            
            elapsed = time.time() - start
            await asyncio.sleep(max(0, ROOT_SCAN_INTERVAL - elapsed))

    async def _check_monitored_symbols(self):
        to_remove = []
        newly_aligned = []
        for sym, info in list(self._mtf_monitoring.items()):
            price = self._last_price_cache.get(sym) or await self.client.get_latest_price(sym)
            if not price: continue
            
            mtf = self._compute_mtf_alignment(sym, price)
            if mtf["status"] in ("aligned", "daily_rising"):
                to_remove.append(sym)
                tv_score, tv_label = self.compute_tv_rating(sym, info["root"], price)
                newly_aligned.append({"symbol": sym, "root": info["root"], "price": price, "tv_score": tv_score, "tv_label": tv_label, "from_monitoring": True})
            else:
                # FIX: Silent logging prevents spamming telegram during MTF re-alignment
                logger.info("MTF Update for %s: neg=%s", sym, mtf.get("negative_tfs"))
        
        for sym in to_remove: self._mtf_monitoring.pop(sym, None)
        if newly_aligned: await self.handle_root_signals(newly_aligned, allow_open_trades=True)

    async def handle_root_signals(self, root_signals: List[Dict[str, Any]], allow_open_trades: bool = True) -> List[Dict[str, Any]]:
        evaluated = []
        for item in root_signals:
            sym, price, root = item["symbol"], item["price"], item["root"]
            mtf = self._compute_mtf_alignment(sym, price)
            
            if mtf["status"] in ("aligned", "daily_rising"):
                item.update({"accept": True, "reason": "aligned"})
                evaluated.append(item)
            elif mtf["status"] == "monitoring":
                if sym not in self._mtf_monitoring:
                    self._mtf_monitoring[sym] = {"root": root, "started_at": time.time()}
                item.update({"accept": False, "reason": "monitoring"})
                evaluated.append(item)
        return evaluated

    async def send_summary(self, root_signals: List[Dict[str, Any]], evaluated: Optional[List[Dict[str, Any]]] = None, full_push: bool = False):
        now_ts = time.time()
        
        # FIX: Deduplication based on candle start_at
        if full_push:
            for sig in root_signals:
                sym, rt = sig.get("symbol"), sig.get("root")
                start_at = sig.get("start_at", now_ts)
                
                # Check if we have already alerted for this specific candle
                if self._last_root_signal_send.get((sym, rt)) != start_at:
                    vol_str = f"{sig.get('vol_change', 0):+.2f}%"
                    block = (
                        f"📌 Bybit Perp | {rt} Signal\n"
                        f"Symbol: {sym}\n"
                        f"Price: {sig['price']}\n"
                        f"TV Rating: {sig.get('tv_label')} ({sig.get('tv_score', 0):+.3f})\n"
                        f"24h Vol Δ: {vol_str}"
                    )
                    await send_message(block)
                    self._last_root_signal_send[(sym, rt)] = start_at
        else:
            # Minimal/delta push logic (if used)
            pass

    async def run(self):
        self._task = asyncio.create_task(self.root_scan_loop())
        await self._task

    def stop(self):
        self._stop = True
        if self._task: self._task.cancel()
