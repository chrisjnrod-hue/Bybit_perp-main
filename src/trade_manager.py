import time
import math
from typing import Dict, Any, List, Optional
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, getcontext
from .logger import get_logger
from .config import (
    MAX_OPEN_TRADES, TP_PERCENT, SL_PERCENT, BREAKEVEN_PERCENT,
    BREAKEVEN_TRIGGER_PERCENT, BREAKEVEN_HL, POSITION_SIZING_MODE, 
    FIXED_QTY, RISK_PERCENT_PER_TRADE, MAX_LOSS_PER_TRADE
)

getcontext().prec = 28
logger = get_logger("trade_manager")


class Trade:
    """
    Represents a single open trade with full money management tracking.
    Supports:
    - Take-profit and stop-loss levels
    - 3-tier Break-even system:
      1. BREAKEVEN_PERCENT: Normal BE at profit threshold
      2. BREAKEVEN_TRIGGER_PERCENT: Triggers normal BE activation
      3. BREAKEVEN_HL: Continuous trailing BE (moves to higher lows)
    - Trade lifecycle: open -> managed -> closed
    """
    def __init__(self, symbol: str, side: str, entry_price: float, qty: float, meta: dict):
        self.symbol = symbol
        self.side = side
        self.entry_price = float(entry_price)
        self.qty = float(qty)
        self.meta = meta or {}
        self.open_time = time.time()
        self.closed = False
        self.close_price: Optional[float] = None
        self.close_time: Optional[float] = None
        self.close_reason: Optional[str] = None
        
        # Money management
        self.tp_price: Optional[float] = None
        self.sl_price: Optional[float] = None
        
        # ============ 3-TIER BREAK-EVEN SYSTEM ============
        
        # 1. BREAKEVEN_PERCENT: The normal break-even price (entry + buffer)
        #    This is the SL that activates when profit threshold is hit
        self.be_price: Optional[float] = None  
        
        # 2. BREAKEVEN_TRIGGER_PERCENT: Profit % threshold that triggers normal BE activation
        #    Once trade profits this %, BE system activates
        self.be_trigger_threshold: Optional[float] = None  
        
        # 3. BREAKEVEN_HL: Continuous trailing break-even (higher lows)
        #    Tracks highest price reached and moves SL to HL as price rises
        self.be_hl_active = False  # Whether HL-based BE is active
        self.be_hl_price: Optional[float] = None  # Current HL-based BE SL
        self.be_hl_highest = self.entry_price  # Track highest price for HL calculation
        
        # State tracking
        self.be_normal_triggered = False  # Normal BE activated (BREAKEVEN_TRIGGER_PERCENT threshold hit)
        self.max_price: Optional[float] = None  # Track max price for profit calculations
        self.pnl: Optional[float] = None
        self.pnl_percent: Optional[float] = None

    def set_risk_levels(
        self, 
        tp_price: float, 
        sl_price: float, 
        be_percent: Optional[float] = None,
        be_trigger_percent: Optional[float] = None
    ):
        """
        Set take-profit, stop-loss, and break-even prices.
        
        Args:
            tp_price: Take-profit price
            sl_price: Initial stop-loss price
            be_percent: Normal break-even price (entry + buffer)
            be_trigger_percent: Profit % that triggers normal BE
        """
        self.tp_price = float(tp_price)
        self.sl_price = float(sl_price)
        
        if be_percent is not None:
            self.be_price = float(be_percent)
        
        if be_trigger_percent is not None:
            self.be_trigger_threshold = float(be_trigger_percent)
        
        # Initialize HL-based BE at entry
        self.be_hl_highest = float(self.entry_price)

    def update_max_price(self, current_price: float):
        """Update max price for monitoring profit thresholds and HL tracking"""
        current = float(current_price)
        if self.max_price is None:
            self.max_price = current
        else:
            self.max_price = max(self.max_price, current)
        
        # Update HL tracking (for BREAKEVEN_HL continuous BE)
        self.be_hl_highest = max(self.be_hl_highest, current)

    def get_current_profit_percent(self, current_price: float) -> float:
        """Calculate current profit percentage"""
        current = float(current_price)
        if self.entry_price <= 0:
            return 0.0
        profit = current - self.entry_price
        return (profit / self.entry_price) * 100.0

    # ============================================================================
    # BREAK-EVEN SYSTEM: 3 TIERS
    # ============================================================================

    def check_normal_breakeven_trigger(self, current_price: float) -> bool:
        """
        TIER 2: Check if BREAKEVEN_TRIGGER_PERCENT threshold is hit.
        
        When this fires, normal break-even (TIER 1) gets activated.
        Returns True if BE should now be active.
        
        Example: If BREAKEVEN_TRIGGER_PERCENT=3%, and profit reaches 3%,
        this returns True and SL moves to entry price.
        """
        if self.be_normal_triggered or self.be_trigger_threshold is None:
            return False
        
        self.update_max_price(current_price)
        
        if self.max_price <= self.entry_price:
            return False
        
        profit_pct = self.get_current_profit_percent(self.max_price)
        
        if profit_pct >= self.be_trigger_threshold:
            self.be_normal_triggered = True
            logger.info(
                "[BREAKEVEN_TRIGGER] %s: Profit %.2f%% >= Trigger %.2f%% | Normal BE activated",
                self.symbol, profit_pct, self.be_trigger_threshold
            )
            return True
        
        return False

    def activate_breakeven_hl(self):
        """
        TIER 3: Activate continuous higher-lows break-even.
        
        Once activated, SL will continuously move to higher lows as price rises.
        This is a trailing stop that protects gains while allowing upside.
        """
        self.be_hl_active = True
        self.be_hl_price = self.entry_price
        logger.info(
            "[BREAKEVEN_HL_ACTIVATED] %s: Continuous HL-based BE now active at %.8f",
            self.symbol, self.be_hl_price
        )

    def update_breakeven_hl(self, current_price: float, hl_buffer_pct: float = 0.1) -> bool:
        """
        TIER 3: Update continuous HL-based break-even.
        
        Moves SL to follow higher lows as price rises, protecting gains.
        
        Args:
            current_price: Current market price
            hl_buffer_pct: Buffer % above HL to place SL (prevents whipsaw)
        
        Returns:
            True if HL-BE SL was updated
        """
        if not self.be_hl_active:
            return False
        
        current = float(current_price)
        self.update_max_price(current)
        
        # Calculate HL-based SL (small buffer above entry to prevent whipsaw)
        buffer = self.entry_price * (hl_buffer_pct / 100.0)
        new_hl_price = self.entry_price + buffer
        
        if self.be_hl_price is None or new_hl_price > self.be_hl_price:
            old_hl = self.be_hl_price or self.entry_price
            self.be_hl_price = new_hl_price
            if new_hl_price > old_hl:
                logger.debug(
                    "[BREAKEVEN_HL_UPDATE] %s: SL moved from %.8f -> %.8f (HL trailing)",
                    self.symbol, old_hl, new_hl_price
                )
                return True
        
        return False

    def get_active_sl(self) -> Optional[float]:
        """
        Get the currently active stop-loss considering all BE types.
        
        Priority (highest SL wins):
        1. HL-based BE (TIER 3) if active - continuous trailing
        2. Normal BE (TIER 1) if triggered - fixed at entry + buffer
        3. Initial SL - original stop-loss
        """
        # 1. HL-based BE takes priority (trailing)
        if self.be_hl_active and self.be_hl_price is not None:
            return self.be_hl_price
        
        # 2. Normal BE if triggered
        if self.be_normal_triggered and self.be_price is not None:
            return self.be_price
        
        # 3. Initial SL
        return self.sl_price

    def close(self, price: float, reason: str = "manual"):
        """Close the trade and compute P&L"""
        self.closed = True
        self.close_price = float(price)
        self.close_time = time.time()
        self.close_reason = reason
        
        # Compute P&L
        if self.side.lower() == "buy":
            pnl = (self.close_price - self.entry_price) * self.qty
        else:
            pnl = (self.entry_price - self.close_price) * self.qty
        
        self.pnl = pnl
        if self.entry_price > 0:
            self.pnl_percent = (pnl / (self.entry_price * self.qty)) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Export trade state to dict"""
        return {
            "symbol": self.symbol,
            "side": self.side,
            "entry": self.entry_price,
            "qty": self.qty,
            "open_time": self.open_time,
            "tp": self.tp_price,
            "sl": self.sl_price,
            "be_normal": self.be_price,
            "be_trigger_threshold": self.be_trigger_threshold,
            "be_normal_triggered": self.be_normal_triggered,
            "be_hl_active": self.be_hl_active,
            "be_hl_price": self.be_hl_price,
            "be_hl_highest": self.be_hl_highest,
            "max_price": self.max_price,
            "closed": self.closed,
            "close_price": self.close_price,
            "close_time": self.close_time,
            "close_reason": self.close_reason,
            "pnl": self.pnl,
            "pnl_percent": self.pnl_percent
        }


class TradeManager:
    """
    Complete trade lifecycle management with 3-tier break-even system:
    - Position sizing (fixed, auto, risk-based)
    - Stop-loss & take-profit calculation
    - TIER 1: Normal break-even at entry + buffer
    - TIER 2: Trigger for normal BE (profit threshold)
    - TIER 3: Continuous HL-based trailing break-even
    - Risk-per-trade limits
    - Trade tracking and P&L aggregation
    """
    
    def __init__(self):
        self.open_trades: List[Trade] = []
        self.closed_trades: List[Trade] = []
        self.total_pnl = 0.0
        self.win_count = 0
        self.loss_count = 0

    def can_open(self) -> bool:
        """Check if new positions can be opened"""
        return len(self.open_trades) < MAX_OPEN_TRADES

    def compute_qty_from_balance(
        self, 
        balance_usdt: Optional[float], 
        price: float, 
        symbol_info: Optional[Dict[str, Any]] = None
    ) -> float:
        """
        Compute quantity (contracts/units) using selected sizing mode.
        
        Modes:
        - fixed: Use FIXED_QTY
        - auto: balance_usdt / MAX_OPEN_TRADES / price
        - risk: Size based on RISK_PERCENT_PER_TRADE (requires symbol_info)
        
        Returns float (raw qty, not quantized).
        """
        if POSITION_SIZING_MODE == "fixed":
            return float(FIXED_QTY)
        
        if not balance_usdt or balance_usdt <= 0:
            logger.warning("Invalid balance for auto sizing, falling back to FIXED_QTY")
            return float(FIXED_QTY)
        
        if POSITION_SIZING_MODE == "risk" and symbol_info:
            return self._compute_risk_based_qty(balance_usdt, price, symbol_info)
        
        # Default: auto mode
        notional = balance_usdt / MAX_OPEN_TRADES
        
        if symbol_info:
            contract_size = symbol_info.get("contract_size")
            if contract_size and contract_size > 0:
                try:
                    qty = notional / float(contract_size)
                    return float(qty)
                except Exception:
                    pass
        
        # Fallback: qty = notional / price
        qty = notional / price if price > 0 else float(FIXED_QTY)
        return round(qty, 8)

    def _compute_risk_based_qty(
        self, 
        balance_usdt: float, 
        entry_price: float, 
        symbol_info: Dict[str, Any]
    ) -> float:
        """
        Size position based on risk percentage.
        
        Formula:
        - Risk amount = balance * RISK_PERCENT_PER_TRADE
        - SL distance = entry * SL_PERCENT / 100
        - Qty = risk_amount / SL_distance
        """
        try:
            risk_amount = balance_usdt * (RISK_PERCENT_PER_TRADE / 100.0)
            
            # Cap by MAX_LOSS_PER_TRADE if set
            if MAX_LOSS_PER_TRADE is not None and MAX_LOSS_PER_TRADE > 0:
                risk_amount = min(risk_amount, MAX_LOSS_PER_TRADE)
            
            # SL distance in price
            sl_distance = entry_price * (SL_PERCENT / 100.0)
            
            if sl_distance <= 0:
                logger.warning("Invalid SL_PERCENT %s; using auto mode", SL_PERCENT)
                return self.compute_qty_from_balance(balance_usdt, entry_price, symbol_info)
            
            # Qty = risk_amount / SL_distance
            qty = risk_amount / sl_distance
            
            # Apply contract size if available
            contract_size = symbol_info.get("contract_size")
            if contract_size and contract_size > 0:
                qty = qty / float(contract_size)
            
            return float(qty)
        except Exception as e:
            logger.exception("Error computing risk-based qty: %s", e)
            return float(FIXED_QTY)

    def compute_tp_and_sl(self, entry_price: float) -> tuple:
        """
        Compute take-profit and stop-loss prices.
        
        TP = entry * (1 + TP_PERCENT / 100)
        SL = entry * (1 - SL_PERCENT / 100)
        """
        tp = entry_price * (1.0 + TP_PERCENT / 100.0)
        sl = entry_price * (1.0 - SL_PERCENT / 100.0)
        return float(tp), float(sl)

    def compute_breakeven_price(self, entry_price: float) -> Optional[float]:
        """
        TIER 1: Compute normal break-even price.
        
        This is where SL moves when BREAKEVEN_TRIGGER_PERCENT is hit.
        Usually entry + small buffer to cover fees and slippage.
        """
        if BREAKEVEN_HL == "entry":
            return float(entry_price)
        elif BREAKEVEN_HL == "high":
            return entry_price * (1.0 + 0.001)  # 0.1% above entry to cover slippage
        else:
            return None

    def open_trade(
        self, 
        symbol: str, 
        side: str, 
        entry_price: float, 
        qty: float, 
        meta: dict = None
    ) -> Optional[Trade]:
        """
        Open a new trade with full 3-tier break-even setup.
        
        Returns Trade object or None if max trades exceeded.
        """
        if not self.can_open():
            logger.info("Max open trades reached (%d). Skipping open.", MAX_OPEN_TRADES)
            return None
        
        trade = Trade(symbol, side, entry_price, qty, meta or {})
        
        # Set risk levels
        tp, sl = self.compute_tp_and_sl(entry_price)
        be_normal = self.compute_breakeven_price(entry_price)
        
        # TIER 1: Normal BE price
        # TIER 2: Profit % threshold to trigger normal BE
        trade.set_risk_levels(tp, sl, be_normal, BREAKEVEN_TRIGGER_PERCENT)
        
        # Activate TIER 3: HL-based BE (starts at entry, moves as price rises)
        trade.activate_breakeven_hl()
        
        self.open_trades.append(trade)
        logger.info(
            "Trade opened %s %s @ %.8f qty=%s TP=%.8f SL=%.8f BE_Normal=%.8f BE_Trigger=%.2f%% BE_HL=Active", 
            symbol, side, entry_price, qty, tp, sl, be_normal or 0, BREAKEVEN_TRIGGER_PERCENT
        )
        return trade

    def close_trade(self, trade: Trade, price: float, reason: str = "manual"):
        """
        Close a trade and track P&L.
        
        Reasons: "tp_hit", "sl_hit", "manual", "be_trigger", "be_hl_triggered", etc.
        """
        trade.close(price, reason)
        logger.info(
            "Closed trade %s @ %.8f (entry %.8f) Reason=%s PnL=%.2f (%.2f%%)", 
            trade.symbol, price, trade.entry_price, reason,
            trade.pnl or 0, trade.pnl_percent or 0
        )
        
        # Update stats
        self.open_trades = [t for t in self.open_trades if not t.closed]
        self.closed_trades.append(trade)
        
        if trade.pnl is not None:
            self.total_pnl += trade.pnl
            if trade.pnl > 0:
                self.win_count += 1
            else:
                self.loss_count += 1

    def check_trade_conditions(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """
        Check if any trade for symbol has hit TP/SL or BE triggers.
        Checks all 3 BE tiers.
        
        Returns dict with actions to take:
        {
            "close_reason": close_reason or None,
            "trade": Trade object or None,
            "price": close price or None,
            "be_update": dict with BE state changes
        }
        """
        for trade in self.open_trades:
            if trade.symbol != symbol:
                continue
            
            be_update = {}
            current = float(current_price)
            trade.update_max_price(current)
            
            # ============ TIER 2 CHECK: Normal BE trigger ============
            if trade.check_normal_breakeven_trigger(current):
                be_update["normal_triggered"] = True
            
            # ============ TIER 3 CHECK: Update HL-based BE ============
            if trade.update_breakeven_hl(current):
                be_update["hl_updated"] = True
            
            # ============ CHECK TP/SL ============
            
            # Check TP
            if trade.tp_price and current >= trade.tp_price:
                return {
                    "close_reason": "tp_hit",
                    "trade": trade,
                    "price": current,
                    "be_update": be_update
                }
            
            # Check active SL (considers all BE tiers)
            active_sl = trade.get_active_sl()
            if active_sl and current <= active_sl:
                close_reason = "sl_hit"
                if trade.be_normal_triggered:
                    close_reason = "be_normal_hit"
                elif trade.be_hl_active:
                    close_reason = "be_hl_hit"
                
                return {
                    "close_reason": close_reason,
                    "trade": trade,
                    "price": current,
                    "be_update": be_update
                }
        
        return {
            "close_reason": None,
            "trade": None,
            "price": None,
            "be_update": {}
        }

    def update_open_trades(self, symbol: str, current_price: float) -> List[Dict[str, Any]]:
        """
        Update all open trades for a symbol and return any that need closing.
        """
        closes = []
        for trade in self.open_trades:
            if trade.symbol != symbol:
                continue
            
            check = self.check_trade_conditions(symbol, current_price)
            if check["close_reason"]:
                closes.append(check)
        
        return closes

    def summary(self) -> List[Dict[str, Any]]:
        """Get summary of all open trades with BE status"""
        out = []
        for t in self.open_trades:
            be_status = "none"
            if t.be_normal_triggered:
                be_status = "normal_active"
            elif t.be_hl_active:
                be_status = "hl_active"
            
            out.append({
                "symbol": t.symbol,
                "side": t.side,
                "entry": t.entry_price,
                "qty": t.qty,
                "open_time": t.open_time,
                "tp": t.tp_price,
                "sl": t.sl_price,
                "be_normal": t.be_price,
                "be_status": be_status,
                "be_hl_sl": t.be_hl_price,
                "max_price": t.max_price
            })
        return out

    def get_stats(self) -> Dict[str, Any]:
        """Get trading statistics"""
        total_trades = self.win_count + self.loss_count
        win_rate = (self.win_count / total_trades * 100) if total_trades > 0 else 0
        
        return {
            "total_pnl": self.total_pnl,
            "total_trades": total_trades,
            "wins": self.win_count,
            "losses": self.loss_count,
            "win_rate": win_rate,
            "open_trades": len(self.open_trades),
            "closed_trades": len(self.closed_trades)
        }

    def export_trades(self, closed_only: bool = False) -> List[Dict[str, Any]]:
        """Export trade history to JSON-serializable format"""
        trades = self.closed_trades if closed_only else (self.open_trades + self.closed_trades)
        return [t.to_dict() for t in trades]
