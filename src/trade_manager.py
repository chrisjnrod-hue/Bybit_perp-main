import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Any
import httpx

logger = logging.getLogger("TradeManager")

class TradeManager:
    def __init__(self, exchange_client, config):
        """
        Initializes the TradeManager with an exchange client and configuration (module or dict).
        """
        self.exchange = exchange_client
        self.config = config
        
        if isinstance(config, dict):
            self.state_file = config.get("STATE_FILE", "open_trades.json")
        else:
            self.state_file = getattr(config, "STATE_FILE", "open_trades.json")
            
        self.open_trades: List[Dict] = []
        self.load_state()

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Helper to retrieve configuration values whether config is passed as a module or dictionary."""
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        return getattr(self.config, key, default)

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

    def can_open(self) -> bool:
        """Checks if current open trades count is below the maximum allowed."""
        max_open = self._get_config("MAX_OPEN_TRADES", 5)
        return len(self.open_trades) < max_open

    async def send_telegram_alert(self, message: str):
        """Pushes trade management and execution alerts to Telegram."""
        token = self._get_config("TELEGRAM_BOT_TOKEN")
        chat_id = self._get_config("TELEGRAM_CHAT_ID")
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

    def compute_qty_from_balance(self, balance_usdt: Optional[float], price: float, symbol_info: Optional[dict] = None) -> float:
        """Computes position quantity based on account balance divided by available trading slots."""
        max_open_trades = self._get_config("MAX_OPEN_TRADES", 5)
        if max_open_trades <= 0 or price <= 0:
            return 0.0
        if not balance_usdt or balance_usdt <= 0:
            notional = 100.0  # fallback default notional amount
        else:
            notional = balance_usdt / max_open_trades
        return notional / price

    def open_trade(self, symbol: str, side: str, price: float, qty: float, metadata: Optional[dict] = None) -> dict:
        """Records and persists an executed or simulated trade position."""
        tp_pct = self._get_config("TP_PERCENT", 2.0)
        sl_pct = self._get_config("SL_PERCENT", 1.0)
        
        if side.upper() == "BUY":
            initial_sl = price * (1 - sl_pct / 100)
            initial_tp = price * (1 + tp_pct / 100)
        else:
            initial_sl = price * (1 + sl_pct / 100)
            initial_tp = price * (1 - tp_pct / 100)

        trade_record = {
            "symbol": symbol,
            "side": side,
            "entry_price": price,
            "qty": qty,
            "notional": price * qty,
            "current_sl": initial_sl,
            "current_tp": initial_tp,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "breakeven_triggered": False,
            "highest_price": price,
            "metadata": metadata or {}
        }
        
        self.open_trades.append(trade_record)
        self.save_state()
        logger.info(f"Successfully recorded trade for {symbol} at entry {price}, qty {qty}")
        return trade_record

    async def monitor_trades_loop(self):
        """Background polling loop to actively monitor open positions for TP, SL, Breakeven, and Higher Lows trailing."""
        be_trigger = self._get_config("BREAKEVEN_TRIGGER_PERCENT", 0.5)
        be_higher_lows = self._get_config("BREAKEVEN_HIGHER_LOWS", True)
        sl_pct = self._get_config("SL_PERCENT", 1.0)

        while True:
            try:
                for trade in list(self.open_trades):
                    symbol = trade["symbol"]
                    side = trade["side"]
                    entry = trade["entry_price"]

                    if not hasattr(self.exchange, "get_ticker"):
                        continue
                    ticker = await self.exchange.get_ticker(symbol)
                    current_price = float(ticker.get("lastPrice", ticker.get("price", 0)))
                    if current_price <= 0:
                        continue

                    if side.upper() == "BUY":
                        pnl_pct = ((current_price - entry) / entry) * 100
                        hit_tp = current_price >= trade["current_tp"]
                        hit_sl = current_price <= trade["current_sl"]
                        if current_price > trade.get("highest_price", entry):
                            trade["highest_price"] = current_price
                    else:
                        pnl_pct = ((entry - current_price) / entry) * 100
                        hit_tp = current_price <= trade["current_tp"]
                        hit_sl = current_price >= trade["current_sl"]
                        if current_price < trade.get("highest_price", entry):
                            trade["highest_price"] = current_price

                    if hit_tp or hit_sl:
                        close_reason = "Take Profit (TP)" if hit_tp else "Stop Loss (SL)"
                        logger.info(f"Closing trade {symbol} due to {close_reason} at price {current_price}")
                        
                        try:
                            if hasattr(self.exchange, "close_position"):
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

                    if not trade.get("breakeven_triggered", False) and pnl_pct >= be_trigger:
                        trade["breakeven_triggered"] = True
                        new_sl = entry * (1 + 0.0005) if side.upper() == "BUY" else entry * (1 - 0.0005)
                        trade["current_sl"] = new_sl
                        if hasattr(self.exchange, "update_stop_loss"):
                            await self.exchange.update_stop_loss(symbol, new_sl)
                        self.save_state()
                        await self.send_telegram_alert(f"🛡️ *Breakeven Triggered*\nSymbol: `{symbol}`\nSL: `{new_sl}`")

                    elif trade.get("breakeven_triggered", False) and be_higher_lows:
                        if side.upper() == "BUY":
                            potential_sl = trade["highest_price"] * (1 - sl_pct / 100)
                            if potential_sl > trade["current_sl"]:
                                trade["current_sl"] = potential_sl
                                if hasattr(self.exchange, "update_stop_loss"):
                                    await self.exchange.update_stop_loss(symbol, potential_sl)
                                self.save_state()
                        else:
                            potential_sl = trade["highest_price"] * (1 + sl_pct / 100)
                            if potential_sl < trade["current_sl"]:
                                trade["current_sl"] = potential_sl
                                if hasattr(self.exchange, "update_stop_loss"):
                                    await self.exchange.update_stop_loss(symbol, potential_sl)
                                self.save_state()

            except Exception as e:
                logger.error(f"Error in monitor_trades_loop: {e}")

            await asyncio.sleep(5)
