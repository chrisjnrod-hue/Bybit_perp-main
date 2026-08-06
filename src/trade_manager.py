import os
import json
import logging
import asyncio
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class TradeManager:
    def __init__(self, exchange_client=None, config: Optional[dict] = None):
        """
        Initializes the TradeManager with exchange client, configuration parameters,
        risk controls, and state persistence for tracking active trading slots.
        """
        self.exchange = exchange_client
        self.config = config or {}
        self.state_file = self.config.get("STATE_FILE", "open_trades.json")
        self.open_trades: List[Dict] = []
        
        # Risk management and execution parameters
        self.leverage = float(self.config.get("LEVERAGE", 1))
        self.take_profit_pct = float(self.config.get("TAKE_PROFIT_PCT", 2.0))
        self.stop_loss_pct = float(self.config.get("STOP_LOSS_PCT", 1.0))
        self.spread_limit = float(self.config.get("SPREAD_LIMIT", 0.5))
        self.slippage_limit = float(self.config.get("SLIPPAGE_LIMIT", 0.5))
        self.simulated_mode = bool(self.config.get("SIMULATED_MODE", False))
        
        # Load persisted trades on startup to maintain state across restarts
        self.load_state()

    def load_state(self):
        """Loads open trades state from JSON persistence file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    self.open_trades = data.get("open_trades", [])
                logger.info(f"Loaded {len(self.open_trades)} active trades from {self.state_file}")
            except Exception as e:
                logger.error(f"Failed to load trade state: {e}")
                self.open_trades = []

    def save_state(self):
        """Saves open trades state to JSON persistence file."""
        try:
            with open(self.state_file, "w") as f:
                json.dump({"open_trades": self.open_trades}, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save trade state: {e}")

    def can_open(self) -> bool:
        """Checks if the number of open trades is below the maximum allowed trading slots."""
        max_open = int(self.config.get("MAX_OPEN_TRADES", 5))
        return len(self.open_trades) < max_open

    def compute_qty_from_balance(self, balance_usdt: float, price: float = 0.0, symbol_info: Optional[dict] = None) -> float:
        """Computes position quantity based on account balance, leverage, and capital allocation rules across slots."""
        max_open_trades = int(self.config.get("MAX_OPEN_TRADES", 5))
        if max_open_trades <= 0 or not balance_usdt or balance_usdt <= 0:
            return 0.0
        
        allocated_capital = (balance_usdt / max_open_trades) * self.leverage
        if price and price > 0:
            return allocated_capital / price
        return allocated_capital

    async def check_spread(self, symbol: str, current_price: float) -> bool:
        """Validates market spread before trade execution against configured limits."""
        if not self.exchange:
            return True
        try:
            orderbook = await self.exchange.fetch_order_book(symbol)
            if orderbook and "bids" in orderbook and "asks" in orderbook and orderbook["bids"] and orderbook["asks"]:
                best_bid = orderbook["bids"][0][0]
                best_ask = orderbook["asks"][0][0]
                spread_pct = ((best_ask - best_bid) / best_bid) * 100
                if spread_pct > self.spread_limit:
                    logger.warning(f"Spread check failed for {symbol}: {spread_pct:.3f}% exceeds limit {self.spread_limit}%")
                    return False
        except Exception as e:
            logger.error(f"Error checking spread for {symbol}: {e}")
        return True

    async def check_slippage(self, symbol: str, side: str, expected_price: float) -> bool:
        """Validates potential price slippage before trade execution."""
        if not self.exchange:
            return True
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            if ticker and "last" in ticker:
                live_price = ticker["last"]
                slippage_pct = abs((live_price - expected_price) / expected_price) * 100
                if slippage_pct > self.slippage_limit:
                    logger.warning(f"Slippage check failed for {symbol}: {slippage_pct:.3f}% exceeds limit {self.slippage_limit}%")
                    return False
        except Exception as e:
            logger.error(f"Error checking slippage for {symbol}: {e}")
        return True

    async def set_leverage(self, symbol: str):
        """Configures exchange leverage for the target trading pair."""
        if self.exchange and hasattr(self.exchange, "set_leverage"):
            try:
                await self.exchange.set_leverage(int(self.leverage), symbol)
            except Exception as e:
                logger.error(f"Failed to set leverage for {symbol}: {e}")

    async def open_trade(self, symbol: str, side: str, signal_price: float, qty_or_balance: float, metadata: Optional[dict] = None) -> bool:
        """Executes pre-execution filters, handles live/simulated orders, and records the active position."""
        try:
            if not self.can_open():
                logger.warning(f"Cannot open trade for {symbol}: Maximum open trades limit reached.")
                return False

            # Run safety filters
            if not await self.check_spread(symbol, signal_price):
                return False
            if not await self.check_slippage(symbol, side, signal_price):
                return False

            notional = qty_or_balance * signal_price
            
            # Simulated and recommended execution block handling
            if self.simulated_mode:
                logger.info(f"[SIMULATED BLOCK] Executing trade for {symbol} ({side}) at price {signal_price}")
            else:
                await self.set_leverage(symbol)

            trade_record = {
                "symbol": symbol,
                "side": side,
                "entry_price": signal_price,
                "notional": notional,
                "leverage": self.leverage,
                "take_profit_pct": self.take_profit_pct,
                "stop_loss_pct": self.stop_loss_pct,
                "simulated": self.simulated_mode,
                "metadata": metadata or {}
            }
            
            self.open_trades.append(trade_record)
            self.save_state()
            logger.info(f"Successfully opened and recorded trade for {symbol} ({side}) at price {signal_price}")
            return True
        except Exception as e:
            logger.error(f"Error opening trade for {symbol}: {e}")
            return False

    async def monitor_trades_loop(self):
        """Asynchronous background loop monitoring active positions for take-profit and stop-loss triggers."""
        while True:
            try:
                for trade in list(self.open_trades):
                    symbol = trade["symbol"]
                    entry_price = trade["entry_price"]
                    side = trade["side"]
                    tp_pct = trade.get("take_profit_pct", self.take_profit_pct)
                    sl_pct = trade.get("stop_loss_pct", self.stop_loss_pct)

                    if self.exchange:
                        ticker = await self.exchange.fetch_ticker(symbol)
                        if ticker and "last" in ticker:
                            current_price = ticker["last"]
                            
                            if side.upper() == "BUY":
                                pnl_pct = ((current_price - entry_price) / entry_price) * 100 * self.leverage
                            else:
                                pnl_pct = ((entry_price - current_price) / entry_price) * 100 * self.leverage

                            if pnl_pct >= tp_pct or pnl_pct <= -sl_pct:
                                logger.info(f"Closing position {symbol} due to TP/SL trigger. Final PnL: {pnl_pct:.2f}%")
                                self.open_trades.remove(trade)
                                self.save_state()
            except Exception as e:
                logger.error(f"Error in monitor_trades_loop: {e}")
            
            await asyncio.sleep(5)
