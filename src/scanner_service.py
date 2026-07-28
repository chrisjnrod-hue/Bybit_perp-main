# scanner_service.py
# Top-level wrapper that composes the scan and trade modules and exposes Scanner
import time
import asyncio
from typing import Optional

from .logger import get_logger
from .bybit_client import BybitClient
from .ratelimiter import TokenBucket
from .trade_manager import TradeManager

from .scanner_scan import ScannerScan
from .scanner_trade import TradeEvaluator

logger = get_logger("scanner")

class Scanner:
    """
    Backwards-compatible Scanner class that composes:
      - ScannerScan: scanning, kline storage, MACD computation, signals detection
      - TradeEvaluator: scoring and trade open logic
    """
    def __init__(self):
        # Shared client and rate limiter
        self.rate_limiter = TokenBucket(max(1.0, float(1)))
        self.client = BybitClient(rate_limiter=self.rate_limiter)
        self.trade_manager = TradeManager()

        # Instantiate modules, pass shared dependencies
        self.scan = ScannerScan(client=self.client, rate_limiter=self.rate_limiter)
        self.trade_eval = TradeEvaluator(
            client=self.client,
            trade_manager=self.trade_manager,
            quantize_qty_fn=self.scan._quantize_qty,
            compute_macd_for_fn=self.scan.compute_macd_for,
            compute_24h_volume_change_fn=self.scan.compute_24h_volume_change,
            compute_mtf_alignment_fn=self.scan._compute_mtf_alignment,
            compute_tv_rating_fn=self.scan.compute_tv_rating,
            send_message_fn=None  # will import lazily in handle to avoid circular imports
        )

        # Wire events: scan will call back to this Scanner when it has signals
        # Use simple callback registration
        self.scan.register_callback(self._on_event)

        # Task bookkeeping for run/stop compatibility
        self._task: Optional[asyncio.Task] = None

        logger.info("Scanner composed (scan + trade evaluator initialized)")

    async def _on_event(self, event: str, payload):
        """
        Handle events emitted by scan module. We expect at least 'root_signals_ready'
        with payload: {"root_signals": [...], "allow_open_trades": bool}
        """
        try:
            if event == "root_signals_ready":
                root_signals = payload.get("root_signals", [])
                allow_open_trades = payload.get("allow_open_trades", True)
                # Lazily set send_message here to avoid import coupling
                if self.trade_eval.send_message_fn is None:
                    from .telegram import send_message
                    self.trade_eval.send_message_fn = send_message

                evaluated = await self.trade_eval.handle_root_signals(root_signals, allow_open_trades=allow_open_trades)
                # emit evaluated back to scan as event
                await self.scan._emit_event("candidates_evaluated_result", evaluated)
        except Exception:
            logger.exception("Error handling event from scan module")

    async def run(self):
        # Start the scan loop from scanner_scan; it will emit events that we'll handle.
        self._task = asyncio.create_task(self.scan.root_scan_loop())
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
        logger.info("Stopping scanner wrapper...")
        # delegate to scan module stop machinery
        self.scan.stop()

# Backwards compatibility: `from .scanner_service import Scanner` still works.
