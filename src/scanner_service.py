# scanner_service.py (NEW - thin coordinator)
# Thin wrapper that coordinates ScannerOrchestrator and ScannerEvaluator

import asyncio
import time
import os
from typing import List, Dict, Any, Optional

from .logger import get_logger
from .bybit_client import BybitClient
from .trade_manager import TradeManager
from .ratelimiter import TokenBucket
from .scanner_orchestrator import ScannerOrchestrator
from .scanner_evaluator import ScannerEvaluator
from .config import ROOT_TFS, ROOT_SCAN_INTERVAL

logger = get_logger("scanner")

DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")


class Scanner:
    """Main Scanner class coordinating orchestrator and evaluator."""
    
    def __init__(self):
        self.rate_limiter = TokenBucket(max(1.0, float(1)))
        self.client = BybitClient(rate_limiter=self.rate_limiter)
        self.trade_manager = TradeManager()
        
        self.orchestrator = ScannerOrchestrator(
            client=self.client,
            trade_manager=self.trade_manager,
            rate_limiter=self.rate_limiter
        )
        
        self.evaluator = ScannerEvaluator(
            kline_store=self.orchestrator.kline_store,
            trade_manager=self.trade_manager,
            client=self.client,
            last_price_cache=self.orchestrator._last_price_cache
        )
        
        self._stop = False
        self._task: Optional[asyncio.Task] = None

        logger.info("Scanner initialized - ready to run")

    def register_callback(self, cb):
        """Register callback for events from orchestrator."""
        self.orchestrator.register_callback(cb)

    async def root_scan_loop(self):
        """Main scanning loop - detects signals and evaluates trades."""
        logger.info("[DIAGNOSTIC] root_scan_loop: STARTING - interval=%s", ROOT_SCAN_INTERVAL)
        loop_count = 0

        while not self._stop:
            loop_count += 1
            logger.info("[DIAGNOSTIC] root_scan_loop: Beginning scan cycle #%d", loop_count)
            start = time.time()

            try:
                # 1. Discover symbols if needed
                if not self.orchestrator.symbols:
                    logger.info("[DIAGNOSTIC] root_scan_loop: Discovering symbols...")
                    await self.orchestrator.discover_symbols()
                    if self.orchestrator.symbols:
                        logger.info("[DIAGNOSTIC] root_scan_loop: Starting symbol seed (count=%d)", len(self.orchestrator.symbols))
                        await self.orchestrator.seed_all()
                        logger.info("[DIAGNOSTIC] root_scan_loop: Symbol seeding complete")
                    else:
                        logger.warning("[DIAGNOSTIC] root_scan_loop: Symbol discovery returned empty!")
                        await asyncio.sleep(10)
                        continue

                # 2. Ensure REST poller running
                await self.orchestrator._ensure_rest_poller()

                # 3. Detect root signals
                root_signals: List[Dict[str, Any]] = []
                logger.info("[DIAGNOSTIC] root_scan_loop: Checking %d symbols", len(self.orchestrator.symbols))

                async def check_symbol(sym: str):
                    try:
                        # Get current price
                        price = await self.client.get_latest_price(sym)
                        if price is None:
                            return
                        
                        self.orchestrator._last_price_cache[sym] = price
                        
                        # Update volume tracking
                        await self.orchestrator._update_24h_volume(sym)

                        # Check each root timeframe for MACD flip
                        for root in ROOT_TFS:
                            macd_line, sig, hist = self.evaluator.compute_macd_for(
                                sym, root, include_price=price, use_ws_current=True
                            )
                            
                            if not hist or len(hist) < 2:
                                continue
                            
                            # Detect zero-cross flip
                            prev = hist[-2]
                            cur = hist[-1]
                            flip = prev is not None and prev <= 0 and cur is not None and cur > 0
                            
                            if flip:
                                vol_change = self.evaluator.compute_24h_volume_change(sym, self.orchestrator._24h_volumes)
                                tv_score, tv_label = self.evaluator.compute_tv_rating(sym, root, price)
                                
                                root_signals.append({
                                    "symbol": sym,
                                    "root": root,
                                    "price": price,
                                    "hist": hist,
                                    "vol_change": vol_change,
                                    "tv_score": tv_score,
                                    "tv_label": tv_label
                                })
                                logger.info("SIGNAL DETECTED: %s %s @ %s (tv=%s %+.3f)", sym, root, price, tv_label, tv_score)
                    except Exception:
                        logger.exception("Error checking symbol %s", sym)

                # Process symbols in batches
                for i in range(0, len(self.orchestrator.symbols), 100):
                    if self._stop:
                        break
                    batch = self.orchestrator.symbols[i:i + 100]
                    tasks = [asyncio.create_task(check_symbol(s)) for s in batch]
                    await asyncio.gather(*tasks)

                logger.info("[DIAGNOSTIC] root_scan_loop: Found %d signals", len(root_signals))

                # 4. Check monitored symbols for alignment resolution
                await self.evaluator._check_monitored_symbols(self.orchestrator._24h_volumes)

                # 5. Evaluate candidates and apply gates
                evaluated = await self.evaluator.handle_root_signals(
                    root_signals, 
                    self.orchestrator._24h_volumes
                )

                # 6. Send Telegram summaries
                await self.evaluator.send_summary(root_signals, evaluated=evaluated)

            except Exception:
                logger.exception("Error in root scan loop")

            # 7. Sleep until next interval
            elapsed = time.time() - start
            if ROOT_SCAN_INTERVAL:
                to_sleep = max(0, ROOT_SCAN_INTERVAL - elapsed)
                logger.info("[DIAGNOSTIC] root_scan_loop: Sleeping %.1f seconds", to_sleep)
                await asyncio.sleep(to_sleep)
            else:
                logger.info("[DIAGNOSTIC] root_scan_loop: No interval configured, sleeping 60s")
                await asyncio.sleep(60)

    async def discover_symbols(self) -> List[str]:
        """Public method to discover symbols."""
        return await self.orchestrator.discover_symbols()

    async def run(self):
        """Start the scanner main loop."""
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
        """Stop the scanner and cleanup."""
        logger.info("Stopping scanner...")
        self._stop = True
        if self._task and not self._task.done():
            self._task.cancel()
        self.orchestrator.stop()
