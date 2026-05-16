import time
from typing import Dict, Any, List, Optional
from .logger import get_logger
from .config import MAX_OPEN_TRADES, TP_PERCENT, SL_PERCENT, BREAKEVEN_PERCENT, BREAKEVEN_TRIGGER_PERCENT, BREAKEVEN_HL, POSITION_SIZING_MODE, FIXED_QTY

logger = get_logger("trade_manager")

class Trade:
    def __init__(self, symbol: str, side: str, entry_price: float, qty: float, meta: dict):
        self.symbol = symbol
        self.side = side
        self.entry_price = entry_price
        self.qty = qty
        self.meta = meta
        self.open_time = time.time()
        self.closed = False
        self.close_price = None

class TradeManager:
    def __init__(self):
        self.open_trades: List[Trade] = []

    def can_open(self):
        return len(self.open_trades) < MAX_OPEN_TRADES

    def open_trade(self, symbol: str, side: str, entry_price: float, qty: float, meta: dict = None) -> Optional[Trade]:
        if not self.can_open():
            logger.info("Max open trades reached (%d). Skipping open.", MAX_OPEN_TRADES)
            return None
        t = Trade(symbol, side, entry_price, qty, meta or {})
        self.open_trades.append(t)
        logger.info("Trade opened %s %s @ %s qty=%s", symbol, side, entry_price, qty)
        return t

    def close_trade(self, trade: Trade, price: float):
        trade.closed = True
        trade.close_price = price
        logger.info("Closed trade %s @ %s (entry %s)", trade.symbol, price, trade.entry_price)
        # remove closed trades
        self.open_trades = [t for t in self.open_trades if not t.closed]

    def summary(self):
        out = []
        for t in self.open_trades:
            out.append({
                "symbol": t.symbol,
                "side": t.side,
                "entry": t.entry_price,
                "qty": t.qty,
                "open_time": t.open_time
            })
        return out

    def compute_qty_from_balance(self, balance_usdt: Optional[float], price: float, symbol_info: Optional[Dict[str, Any]] = None) -> float:
        """
        Compute quantity (contracts/units) using selected sizing mode and symbol metadata.
        - auto: use balance_usdt / MAX_OPEN_TRADES as notional per trade.
            For linear perpetuals with contract_size provided: qty = notional / contract_size
            Otherwise: qty = notional / price
        - fixed: use FIXED_QTY
        Returns float (raw qty, not quantized).
        """
        if POSITION_SIZING_MODE == "fixed":
            return float(FIXED_QTY)
        # auto
        if not balance_usdt or balance_usdt <= 0:
            logger.warning("Invalid balance for auto sizing, falling back to FIXED_QTY")
            return float(FIXED_QTY)
        notional = balance_usdt / MAX_OPEN_TRADES
        if symbol_info:
            contract_size = symbol_info.get("contract_size")
            if contract_size:
                # for linear contracts: qty (contracts) = notional / contract_size
                try:
                    qty = notional / float(contract_size)
                    return float(qty)
                except Exception:
                    pass
        # fallback: simple qty = notional / price
        qty = notional / price if price > 0 else float(FIXED_QTY)
        return round(qty, 6)
