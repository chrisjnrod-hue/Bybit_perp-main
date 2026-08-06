import os
import json
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class TradeManager:
    def __init__(self, exchange_client=None, config: Optional[dict] = None):
        """
        Initializes the TradeManager with exchange client, configuration parameters,
        and state persistence for tracking active trading slots.
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
        """Computes position quantity based on account balance and capital allocation rules across slots."""
        max_open_trades = int(self.config.get("MAX_OPEN_TRADES", 5))
        if max_open_trades <= 0 or not balance_usdt or balance_usdt <= 0:
            return 0.0
        
        # Capital allocation per slot with leverage applied
        allocated_capital = (balance_usdt / max_open_trades) * self.leverage
        if price and price > 0:
            return allocated_capital / price
        return allocated_capital

    async def open_trade(self, symbol: str, side: str, signal_price: float, qty_or_balance: float, metadata: Optional[dict] = None) -> bool:
        """Executes and records a new trade with risk management and metadata."""
        try:
            if not self.can_open():
                logger.warning(f"Cannot open trade for {symbol}: Maximum open trades limit reached.")
                return False

            notional = qty_or_balance * signal_price
            trade_record = {
                "symbol": symbol,
                "side": side,
                "entry_price": signal_price,
                "notional": notional,
                "leverage": self.leverage,
                "take_profit": self.take_profit_pct,
                "stop_loss": self.stop_loss_pct,
                "metadata": metadata or {}
            }
            
            self.open_trades.append(trade_record)
            self.save_state()
            logger.info(f"Successfully opened and recorded trade for {symbol} ({side}) at price {signal_price}")
            return True
        except Exception as e:
            logger.error(f"Error opening trade for {symbol}: {e}")
            return False
