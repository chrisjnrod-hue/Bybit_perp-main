import asyncio
import json
import logging
import os
from typing import Dict, List, Optional
import httpx

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

    async def send_telegram_alert(self, message: str):
        """Pushes trade management and execution alerts to Telegram."""
        token = self.config.get("TELEGRAM_BOT_TOKEN")
        chat_id = self.config.get("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            return
        
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, timeout=10.0)
                if response.status_code != 200:
                    logger.error(f"Telegram API error: {response.text}")
        except Exception as e:
            logger.error(f"Failed to send Telegram alert: {e}")

    def compute_qty_from_balance(self, balance_usdt: float, price: float, symbol_info: Optional[Dict] = None) -> float:
        """Computes position notional quantity based on account balance divided by available trading slots."""
        max_open_trades = self.config.get("MAX_OPEN_TRADES", 5)
        if max_open_trades <= 0:
            return 0.0
        notional = balance_usdt / max_open_trades
        if price > 0:
            qty = notional / price
        else:
            qty = 0.0
        return qty

    def can_open(self) -> bool:
        """Check if we can open another trade based on MAX_OPEN_TRADES limit."""
        max_open = self.config.get("MAX_OPEN_TRADES", 5)
        return len(self.open_trades) < max_open

    async def check_market_cap(self, symbol: str) -> bool:
        """Filters symbol by minimum market cap to ensure adequate liquidity before entering trade."""
        min_cap = self.config.get("MIN_MARKET_CAP", 50000000) # Default 50M threshold
        try:
            if hasattr(self.exchange, "get_symbol_info"):
                market_info = await self.exchange.get_symbol_info(symbol)
                market_cap = float(market_info.get("marketCap", 0))
                if market_cap > 0 and market_cap < min_cap:
                    logger.warning(f"Market cap check failed for {symbol}: {market_cap} < min required {min_cap}")
                    await self.send_telegram_alert(f"⚠️ *Trade Filtered Out*\nSymbol: `{symbol}`\nReason: Market cap below liquidity threshold.")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error checking market cap for {symbol}: {e}")
            return True # Fail-open if exchange API lacks direct marketCap endpoint

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

    async def open_trade(self, symbol: str, side: str, signal_price: float, qty: float, metadata: Optional[Dict] = None) -> bool:
        """
        Opens a position with specified parameters.
        Pre-execution checks (market cap, spread, slippage, leverage) should be done before calling this.
        """
        max_open = self.config.get("MAX_OPEN_TRADES", 5)
        if len(self.open_trades) >= max_open:
            logger.warning(f"Max open trades limit reached ({max_open}). Skipping trade for {symbol}.")
            return False

        tp_pct = self.config.get("TP_PERCENT", 2.0)
        sl_pct = self.config.get("SL_PERCENT", 1.0)

        try:
            await self.set_leverage(symbol)

            # For real orders, create the order; for simulated, just record it
            order = metadata.get("order") if metadata else None
            if not order:
                order = {
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "price": signal_price,
                    "status": "simulated"
                }
            
            entry_price = float(order.get("avgPrice", signal_price))
            
            # Calculate initial SL and TP absolute price levels
            if side.upper() in ("BUY", "Buy"):
                initial_sl = entry_price * (1 - sl_pct / 100)
                initial_tp = entry_price * (1 + tp_pct / 100)
            else:
                initial_sl = entry_price * (1 + sl_pct / 100)
                initial_tp = entry_price * (1 - tp_pct / 100)

            # Set exchange-level Stop-Loss and Take-Profit if supported
            if hasattr(self.exchange, "set_position_sl_tp"):
                try:
                    await self.exchange.set_position_sl_tp(symbol, stop_loss=initial_sl, take_profit=initial_tp)
                except Exception as e:
                    logger.warning(f"Could not set SL/TP on exchange for {symbol}: {e}")

            trade_record = {
                "symbol": symbol,
                "side": side,
                "entry_price": entry_price,
                "qty": qty,
                "notional": qty * entry_price,
                "current_sl": initial_sl,
                "current_tp": initial_tp,
                "tp_pct": tp_pct,
                "sl_pct": sl_pct,
                "breakeven_triggered": False,
                "highest_price": entry_price,  # Used for tracking higher lows/highs trailing SL
                "opened_at": asyncio.get_event_loop().time() if asyncio.get_event_loop() else 0,
                "metadata": metadata or {}
            }
            
            self.open_trades.append(trade_record)
            self.save_state()
            
            logger.info(f"Successfully opened trade for {symbol} at entry {entry_price}")
            await self.send_telegram_alert(
                f"🟢 *Trade Opened*\n"
                f"Symbol: `{symbol}`\n"
                f"Side: `{side}`\n"
                f"Entry Price: `{entry_price}`\n"
                f"Qty: `{qty:.6f}`\n"
                f"Initial SL: `{initial_sl}` | TP: `{initial_tp}`"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to execute order for {symbol}: {e}")
            return False

    async def monitor_trades_loop(self):
        """Background polling loop to actively monitor open positions for TP, SL, Breakeven, and Higher Lows trailing."""
        be_trigger = self.config.get("BREAKEVEN_TRIGGER_PERCENT", 0.5)
        be_higher_lows = self.config.get("BREAKEVEN_HIGHER_LOWS", True)
        sl_pct = self.config.get("SL_PERCENT", 1.0)

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

                    if side.upper() in ("BUY", "Buy"):
                        pnl_pct = ((current_price - entry) / entry) * 100
                        hit_tp = current_price >= trade["current_tp"]
                        hit_sl = current_price <= trade["current_sl"]
                        
                        # Track highest price for trailing higher lows
                        if current_price > trade.get("highest_price", entry):
                            trade["highest_price"] = current_price
                            
                    else:
                        pnl_pct = ((entry - current_price) / entry) * 100
                        hit_tp = current_price <= trade["current_tp"]
                        hit_sl = current_price >= trade["current_sl"]
                        
                        # Track lowest price for short trailing lows
                        if current_price < trade.get("highest_price", entry):
                            trade["highest_price"] = current_price

                    # Check TP/SL fulfillment
                    if hit_tp or hit_sl:
                        close_reason = "Take Profit (TP)" if hit_tp else "Stop Loss (SL)"
                        logger.info(f"Closing trade {symbol} due to {close_reason} at price {current_price}")
                        
                        try:
                            await self.exchange.close_position(symbol)
                        except Exception as e:
                            logger.error(f"Error executing position close for {symbol}: {e}")

                        self.open_trades.remove(trade)
                        self.save_state()
                        
                        await self.send_telegram_alert(
                            f"🔴 *Trade Closed ({close_reason})*\n"
                            f"Symbol: `{symbol}`\n"
                            f"Exit Price: `{current_price}`\n"
                            f"Final PnL: `{pnl_pct:.2f}%`"
                        )
                        continue

                    # Breakeven and Higher Lows management
                    if not trade.get("breakeven_triggered", False) and pnl_pct >= be_trigger:
                        trade["breakeven_triggered"] = True
                        
                        # Move SL to entry (Breakeven)
                        if side.upper() in ("BUY", "Buy"):
                            new_sl = entry * (1 + 0.0005) # slight buffer above entry fee
                        else:
                            new_sl = entry * (1 - 0.0005)

                        trade["current_sl"] = new_sl
                        if hasattr(self.exchange, "update_stop_loss"):
                            try:
                                await self.exchange.update_stop_loss(symbol, new_sl)
                            except Exception as e:
                                logger.warning(f"Could not update stop loss on exchange for {symbol}: {e}")
                            
                        self.save_state()
                        logger.info(f"Triggered breakeven SL for {symbol} at price {new_sl}")
                        await self.send_telegram_alert(
                            f"🛡️ *Breakeven Triggered*\n"
                            f"Symbol: `{symbol}`\n"
                            f"Stop-Loss moved to entry: `{new_sl}`"
                        )

                    elif trade.get("breakeven_triggered", False) and be_higher_lows:
                        # Automatically trail SL to new higher lows (for BUY) or lower highs (for SELL)
                        if side.upper() in ("BUY", "Buy"):
                            potential_sl = trade["highest_price"] * (1 - sl_pct / 100)
                            if potential_sl > trade["current_sl"]:
                                trade["current_sl"] = potential_sl
                                if hasattr(self.exchange, "update_stop_loss"):
                                    try:
                                        await self.exchange.update_stop_loss(symbol, potential_sl)
                                    except Exception as e:
                                        logger.warning(f"Could not update stop loss on exchange for {symbol}: {e}")
                                self.save_state()
                                logger.info(f"Trailed higher-low SL for {symbol} to {potential_sl}")
                        else:
                            potential_sl = trade["highest_price"] * (1 + sl_pct / 100)
                            if potential_sl < trade["current_sl"]:
                                trade["current_sl"] = potential_sl
                                if hasattr(self.exchange, "update_stop_loss"):
                                    try:
                                        await self.exchange.update_stop_loss(symbol, potential_sl)
                                    except Exception as e:
                                        logger.warning(f"Could not update stop loss on exchange for {symbol}: {e}")
                                self.save_state()
                                logger.info(f"Trailed lower-high SL for {symbol} to {potential_sl}")

            except Exception as e:
                logger.error(f"Error in monitor_trades_loop: {e}")

            await asyncio.sleep(5)
