import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

logger = logging.getLogger("TradeManager")

class TradeManager:
    def __init__(self, exchange_client, config: dict):
        """
        Initializes the TradeManager with an exchange client and configuration parameters.
        """
        self.exchange = exchange_client
        self.config = config
        self.state_file = config.get("STATE_FILE", "open_trades.json")
        self.open_trades: List[Dict] = []
        
        # Load persisted trades on startup to maintain state across restarts
        self.load_state()

    def load_state(self):
        """Loads open trades from local JSON file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    self.open_trades = json.load(f)
                logger.info(f"Loaded {len(self.open_trades)} open trades from state file.")
            except Exception as e:
                logger.error(f"Failed to load trade state file: {e}")
                self.open_trades = []
        else:
            self.open_trades = []

    def save_state(self):
        """Saves current open trades to local JSON file."""
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.open_trades, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save trade state file: {e}")

    def compute_qty_from_balance(self, balance_usdt: float) -> float:
        """Computes position notional quantity based on account balance and max open trades."""
        max_open_trades = self.config.get("MAX_OPEN_TRADES", 5)
        if max_open_trades <= 0:
            return 0.0
        notional = balance_usdt / max_open_trades
        return notional

    async def check_spread(self, symbol: str) -> bool:
        """Validates that the current bid-ask spread is within the allowed threshold."""
        max_spread = self.config.get("MAX_SPREAD_PERCENT", 0.1)
        try:
            ticker = await self.exchange.get_ticker(symbol)
            bid = float(ticker.get("bidPrice", 0))
            ask = float(ticker.get("askPrice", 0))
            if bid <= 0 or ask <= 0:
                return False
            
            spread_pct = ((ask - bid) / bid) * 100
            if spread_pct > max_spread:
                logger.warning(f"Spread check failed for {symbol}: {spread_pct:.4f}% > max {max_spread}%")
                return False
            return True
        except Exception as e:
            logger.error(f"Error checking spread for {symbol}: {e}")
            return False

    async def check_slippage(self, symbol: str, target_price: float, side: str) -> bool:
        """Validates that current market price has not slipped past the maximum allowed threshold."""
        max_slippage = self.config.get("MAX_SLIPPAGE", 0.2)
        try:
            ticker = await self.exchange.get_ticker(symbol)
            current_price = float(ticker.get("lastPrice", 0))
            if current_price <= 0:
                return False
            
            if side.upper() == "BUY":
                slippage_pct = ((current_price - target_price) / target_price) * 100
            else:
                slippage_pct = ((target_price - current_price) / target_price) * 100
                
            if slippage_pct > max_slippage:
                logger.warning(f"Slippage check failed for {symbol}: {slippage_pct:.4f}% > max {max_slippage}%")
                return False
            return True
        except Exception as e:
            logger.error(f"Error checking slippage for {symbol}: {e}")
            return False

    async def set_leverage(self, symbol: str):
        """Enforces the configured leverage on the exchange for the target symbol."""
        leverage = self.config.get("LEVERAGE", 10)
        try:
            await self.exchange.set_leverage(symbol=symbol, leverage=leverage)
            logger.info(f"Leverage successfully set to {leverage}x for {symbol}")
        except Exception as e:
            logger.error(f"Failed to set leverage for {symbol}: {e}")

    async def open_trade(self, symbol: str, side: str, signal_price: float, balance_usdt: float) -> bool:
        """Runs pre-execution checks and opens a live position."""
        max_open = self.config.get("MAX_OPEN_TRADES", 5)
        if len(self.open_trades) >= max_open:
            logger.warning(f"Max open trades limit reached ({max_open}). Skipping trade for {symbol}.")
            return False

        if not await self.check_spread(symbol):
            return False

        if not await self.check_slippage(symbol, signal_price, side):
            return False

        await self.set_leverage(symbol)

        notional = self.compute_qty_from_balance(balance_usdt)
        if notional <= 0:
            logger.error(f"Invalid computed notional size for {symbol}")
            return False

        try:
            order = await self.exchange.create_order(
                symbol=symbol,
                side=side,
                order_type="MARKET",
                notional=notional
            )
            entry_price = float(order.get("avgPrice", signal_price))
            
            trade_record = {
                "symbol": symbol,
                "side": side,
                "entry_price": entry_price,
                "notional": notional,
                "tp_pct": self.config.get("TP_PERCENT", 2.0),
                "sl_pct": self.config.get("SL_PERCENT", 1.0),
                "breakeven_triggered": False
            }
            
            self.open_trades.append(trade_record)
            self.save_state()
            logger.info(f"Successfully opened trade for {symbol} at entry {entry_price}")
            return True
        except Exception as e:
            logger.error(f"Failed to execute order for {symbol}: {e}")
            return False

    async def monitor_trades_loop(self):
        """Background polling loop to actively monitor open positions for TP, SL, and Breakeven."""
        tp_pct = self.config.get("TP_PERCENT", 2.0)
        sl_pct = self.config.get("SL_PERCENT", 1.0)
        be_trigger = self.config.get("BREAKEVEN_TRIGGER_PERCENT", 0.5)

        while True:
            try:
                for trade in list(self.open_trades):
                    symbol = trade["symbol"]
                    side = trade["side"]
                    entry = trade["entry_price"]

                    ticker = await self.exchange.get_ticker(symbol)
                    current_price = float(ticker.get("lastPrice", 0))
                    if current_price <= 0:
                        continue

                    if side.upper() == "BUY":
                        pnl_pct = ((current_price - entry) / entry) * 100
                        hit_tp = current_price >= entry * (1 + tp_pct / 100)
                        hit_sl = current_price <= entry * (1 - sl_pct / 100)
                    else:
                        pnl_pct = ((entry - current_price) / entry) * 100
                        hit_tp = current_price <= entry * (1 - tp_pct / 100)
                        hit_sl = current_price >= entry * (1 + sl_pct / 100)

                    if hit_tp or hit_sl:
                        close_reason = "TP" if hit_tp else "SL"
                        logger.info(f"Closing trade {symbol} due to {close_reason} at price {current_price}")
                        await self.exchange.close_position(symbol)
                        self.open_trades.remove(trade)
                        self.save_state()
                        continue

                    if not trade.get("breakeven_triggered", False) and pnl_pct >= be_trigger:
                        logger.info(f"Triggering breakeven for {symbol} at {current_price} (PnL: {pnl_pct:.2f}%)")
                        trade["breakeven_triggered"] = True
                        self.save_state()

            except Exception as e:
                logger.error(f"Error in monitor_trades_loop: {e}")

            await asyncio.sleep(5)
