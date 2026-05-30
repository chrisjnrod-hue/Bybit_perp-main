# config.py
"""
Production-ready configuration for crypto trading scanner.
Supports full money management, risk controls, and signal filtering.

Load from environment variables using .env file or system ENV.
Safe parsing with defaults to prevent crashes.
"""

import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()


# ============================================================================
# SAFE ENVIRONMENT PARSERS
# ============================================================================

def safe_int_env(name: str, default: int = 0) -> int:
    """Safely parse integer environment variable"""
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default


def safe_float_env(name: str, default: float = 0.0) -> float:
    """Safely parse float environment variable"""
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def safe_bool_env(name: str, default: bool = False) -> bool:
    """Safely parse boolean environment variable"""
    v = os.getenv(name)
    if v is None or v == "":
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def safe_csv_list(name: str, default: Optional[List[str]] = None, sep: str = ",") -> List[str]:
    """Safely parse comma-separated list from environment"""
    v = os.getenv(name)
    if v is None or v == "":
        return default or []
    try:
        return [x.strip() for x in v.split(sep) if x.strip()]
    except Exception:
        return default or []


# ============================================================================
# NETWORK & API KEYS
# ============================================================================

MAINNET = safe_bool_env("MAINNET", True)
"""True = Bybit mainnet, False = testnet"""

USE_WS = safe_bool_env("USE_WS", False)
"""Use WebSocket for real-time updates (reduces REST API calls)"""

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "") or ""
"""Bybit API key for live trading (empty = simulation mode)"""

BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "") or ""
"""Bybit API secret for live trading (empty = simulation mode)"""


# ============================================================================
# SYMBOL FILTERS
# ============================================================================

EXCLUDE_STABLECOINS = safe_csv_list("EXCLUDE_STABLECOINS", ["USDT", "BUSD", "USDC"])
"""List of stablecoins to exclude from symbol discovery"""


# ============================================================================
# TIMEFRAMES & INTERVALS
# ============================================================================

# IMPORTANT: Bybit REST API expects: "1", "5", "15", "60", "240", "D"
# Use numeric values for minutes, "D" for daily
ROOT_TFS = safe_csv_list("ROOT_TFS", ["60", "240", "D"])
"""
Root timeframes for initial signal detection (1h, 4h, 1d).
Scanner triggers when MACD histogram flips positive on these TFs.
"""

MTF_TFS = safe_csv_list("MTF_TFS", ["5", "15", "60", "240", "D"])
"""
Multi-timeframe analysis for MTF filtering (5m, 15m, 1h, 4h, 1d).
Used to confirm signals and calculate MTF alignment score.
"""

ROOT_SCAN_INTERVAL = safe_int_env("ROOT_SCAN_INTERVAL", 0)
"""
Root scan interval in seconds. 
- 0 (default): Run scan at every 5m candle open (aligned)
- >0: Run scan every N seconds (aligned to next interval)
This ensures precise candle-open alignment and reduces redundant checks.
"""

KLINE_SEED_LIMIT = safe_int_env("KLINE_SEED_LIMIT", 200)
"""
Number of historical candles to fetch on startup.
Higher values = more accurate MACD but slower initial load.
Recommended: 150-250 (enough for MACD calculation + buffer)
"""


# ============================================================================
# CONCURRENCY & RATE LIMITING
# ============================================================================

RATE_LIMIT_RPS = safe_float_env("RATE_LIMIT_RPS", 5.0)
"""Rate limit: requests per second to exchange API"""

CONCURRENCY = safe_int_env("CONCURRENCY", 10)
"""Max concurrent symbol checks during scan"""


# ============================================================================
# TRADING ACTIVATION & LIMITS
# ============================================================================

TRADE_ENABLED = safe_bool_env("TRADE_ENABLED", False)
"""
Enable live trading. 
- False: Simulation mode (trades logged but not placed)
- True: Place real orders on Bybit (REQUIRES API_KEY + API_SECRET)
"""

MAX_OPEN_TRADES = safe_int_env("MAX_OPEN_TRADES", 3)
"""
Maximum number of simultaneously open trades.
Scanner will not open new trades once this limit is reached.
"""


# ============================================================================
# POSITION SIZING (MONEY MANAGEMENT)
# ============================================================================

POSITION_SIZING_MODE = os.getenv("POSITION_SIZING_MODE", "auto").strip().lower()
"""
Position sizing strategy:
- "fixed": Use fixed FIXED_QTY for all trades
- "auto": Size = available_balance / MAX_OPEN_TRADES / price
- "risk": Size based on RISK_PERCENT_PER_TRADE and SL distance
Recommended: "risk" for consistent risk management
"""

FIXED_QTY = safe_float_env("FIXED_QTY", 1.0)
"""Fixed quantity per trade (used when POSITION_SIZING_MODE='fixed')"""

RISK_PERCENT_PER_TRADE = safe_float_env("RISK_PERCENT_PER_TRADE", 2.0)
"""
Percent of account balance to risk per trade (0-100).
Example: 2% means if trade hits SL, account loses 2% of balance.
Used when POSITION_SIZING_MODE='risk'
Formula: risk_amount = balance * RISK_PERCENT_PER_TRADE / 100
Qty = risk_amount / SL_distance
"""

MAX_LOSS_PER_TRADE = safe_float_env("MAX_LOSS_PER_TRADE", 0.0)
"""
Maximum loss in USDT per trade (0 = unlimited, cap by risk_percent only).
Hard limit that overrides RISK_PERCENT_PER_TRADE if hit.
Useful to prevent accidental huge losses.
Example: 100 = never risk more than $100 per trade
"""


# ============================================================================
# PROFIT TAKING & STOP LOSS (RISK MANAGEMENT)
# ============================================================================

TP_PERCENT = safe_float_env("TP_PERCENT", 5.0)
"""
Take-profit target as % above entry price.
Example: 5 means TP = entry * 1.05
Trade auto-closes when price reaches or exceeds TP.
"""

SL_PERCENT = safe_float_env("SL_PERCENT", 2.0)
"""
Stop-loss as % below entry price.
Example: 2 means SL = entry * 0.98
Trade auto-closes when price falls to or below SL.
Risk/Reward ratio = TP_PERCENT / SL_PERCENT
Recommended: TP >= 2x SL (2:1 or better)
"""


# ============================================================================
# BREAK-EVEN STOP MANAGEMENT
# ============================================================================

BREAKEVEN_TRIGGER_PERCENT = safe_float_env("BREAKEVEN_TRIGGER_PERCENT", 3.0)
"""
Profit % threshold to activate break-even stop.
Once trade profits >= this %, SL moves to entry (or slightly above).
Example: 3 means when profit reaches 3%, move SL to break-even
Protects gains while allowing continued upside.
"""

BREAKEVEN_HL = os.getenv("BREAKEVEN_HL", "entry").strip().lower()
"""
Break-even stop placement:
- "entry": Move SL exactly to entry price
- "high": Move SL slightly above entry (0.1%)
Recommended: "entry" for simple break-even
"""


# ============================================================================
# SIGNAL FILTERING & SCORING
# ============================================================================

MACD_HIST_THRESHOLD = safe_float_env("MACD_HIST_THRESHOLD", 0.0)
"""
MACD histogram value threshold for signal confirmation.
Usually 0 (flip from negative to positive is the signal).
Higher values require stronger momentum (rarely used in MACD).
"""

MIN_VOLUME_CHANGE = safe_float_env("MIN_VOLUME_CHANGE", 0.0)
"""
Minimum 24h volume change % to accept signal (0-100).
Example: 10 means require 10% volume increase for signal.
Filters low-volume pump signals.
Note: Signal filtering uses volume_change > 0, not as hard reject.
"""


# ============================================================================
# ROOT TIMEFRAME FILTERING
# ============================================================================

ROOT_FILTER = safe_bool_env("ROOT_FILTER", False)
"""
Enable filtering to limit candidates per root TF.
When True: Sort by score and take top ROOT_TOP_N per ROOT_TF.
Prevents spam of similar timeframe signals.
"""

ROOT_TOP_N = safe_int_env("ROOT_TOP_N", 0)
"""
Max candidates per root TF when ROOT_FILTER=True.
0 = no limit (use MAX_OPEN_TRADES).
Example: 2 means max 2 signals per 1h, per 4h, per 1d.
"""
if ROOT_TOP_N <= 0:
    ROOT_TOP_N = MAX_OPEN_TRADES


# ============================================================================
# MULTI-TIMEFRAME (MTF) FILTERING
# ============================================================================

MTF_FILTER = safe_bool_env("MTF_FILTER", True)
"""
Enable advanced MTF alignment filtering.
When True: Evaluates 5m, 15m, 1h, 4h, 1d for signal quality.
Signals require positive MACD alignment across multiple TFs.
Increases signal quality but reduces frequency.
"""

MTF_SLOPE_LOOKBACK = safe_int_env("MTF_SLOPE_LOOKBACK", 3)
"""
Number of candles to look back for MACD histogram slope.
Higher = smoother slope (less noise), but delayed response.
Recommended: 2-5 (3 is good default)
Used to detect if MACD is rising/falling (momentum direction).
"""


# ============================================================================
# TELEGRAM NOTIFICATIONS
# ============================================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
"""
Telegram bot token from @BotFather.
Format: "123456789:ABCdefGHIjklmnoPQRstuvWXYZ-1234567890"
Leave empty to disable Telegram notifications.
"""

TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or ""
"""
Your Telegram chat ID (private or group).
Get it from @userinfobot in Telegram.
Leave empty to disable Telegram notifications.
"""


# ============================================================================
# LOGGING & DEBUG
# ============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
"""
Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
DEBUG = verbose output (scanning details, MACD values, etc.)
INFO = normal operation logs
WARNING = important events only
"""

DEBUG_SURGICAL_LOGS = os.getenv("DEBUG_SURGICAL_LOGS", "").strip().lower() in ("1", "true", "yes", "y")
"""
Enable detailed surgical logs for troubleshooting.
Logs API responses, MACD calculations, flip detection.
Verbose but useful for debugging issues.
"""

DIAGNOSTIC_MODE = os.getenv("DIAGNOSTIC_MODE", "").strip().lower() in ("1", "true", "yes", "y")
"""
Enable diagnostic logging (scanner lifecycle events).
Logs symbol discovery, seeding, scan cycles.
Useful for understanding scanner flow.
"""

SEND_ALL_SIGNALS_ON_DEPLOY = os.getenv("SEND_ALL_SIGNALS_ON_DEPLOY", "true").strip().lower() in ("1", "true", "yes", "y")
"""
On first scan after deploy: send all signals as alert.
Trades only open on NEXT scan cycle (if TRADE_ENABLED=true).
Prevents accidental mass trades on deploy.
"""


# ============================================================================
# VALIDATION & SAFE DEFAULTS
# ============================================================================

def validate_config():
    """Validate configuration for common errors"""
    errors = []
    
    # Check API keys if trading enabled
    if TRADE_ENABLED:
        if not BYBIT_API_KEY or not BYBIT_API_SECRET:
            errors.append("⚠ TRADE_ENABLED=true but BYBIT_API_KEY or BYBIT_API_SECRET missing!")
    
    # Check Telegram if configured
    if TELEGRAM_BOT_TOKEN and not TELEGRAM_CHAT_ID:
        errors.append("⚠ TELEGRAM_BOT_TOKEN set but TELEGRAM_CHAT_ID missing!")
    
    # Check risk parameters
    if POSITION_SIZING_MODE == "risk":
        if RISK_PERCENT_PER_TRADE <= 0 or RISK_PERCENT_PER_TRADE > 50:
            errors.append(f"⚠ RISK_PERCENT_PER_TRADE={RISK_PERCENT_PER_TRADE} seems invalid (should be 0.1-50)")
    
    if TP_PERCENT <= 0:
        errors.append("⚠ TP_PERCENT must be > 0")
    
    if SL_PERCENT <= 0:
        errors.append("⚠ SL_PERCENT must be > 0")
    
    if TP_PERCENT / SL_PERCENT < 1:
        errors.append(f"⚠ Risk/Reward ratio poor: TP/SL = {TP_PERCENT/SL_PERCENT:.2f} (should be ≥2)")
    
    # Check timeframes
    if not ROOT_TFS or len(ROOT_TFS) == 0:
        errors.append("⚠ ROOT_TFS is empty!")
    
    if not MTF_TFS or len(MTF_TFS) == 0:
        errors.append("⚠ MTF_TFS is empty!")
    
    return errors


# Run validation on import
_validation_errors = validate_config()
if _validation_errors:
    import sys
    print("\n" + "="*70)
    print("CONFIG VALIDATION WARNINGS:")
    print("="*70)
    for err in _validation_errors:
        print(f"  {err}")
    print("="*70 + "\n")


# ============================================================================
# CONFIGURATION SUMMARY (for logging)
# ============================================================================

def get_config_summary() -> str:
    """Get human-readable config summary for logs"""
    lines = [
        "╔════════════════════════════════════════════════════════════════════╗",
        "║  SCANNER CONFIGURATION SUMMARY                                    ║",
        "╠════════════════════════════════════════════════════════════════════╣",
        f"║  Network: {'MAINNET' if MAINNET else 'TESTNET':20s} | WS: {'Yes' if USE_WS else 'No':6s} | Trading: {'LIVE' if TRADE_ENABLED else 'SIM':10s}",
        f"║  Root TFs: {','.join(ROOT_TFS):15s} | MTF: {','.join(MTF_TFS):20s}",
        f"║  Scan Interval: {ROOT_SCAN_INTERVAL if ROOT_SCAN_INTERVAL else 'Next 5m':5s} | Max Trades: {MAX_OPEN_TRADES:2d}",
        f"║  Position Sizing: {POSITION_SIZING_MODE:10s} | TP: {TP_PERCENT:5.1f}% | SL: {SL_PERCENT:5.1f}% (RR: {TP_PERCENT/SL_PERCENT:.1f}:1)",
        f"║  Risk/Trade: {RISK_PERCENT_PER_TRADE:5.1f}% | Max Loss: {'Unlimited' if MAX_LOSS_PER_TRADE <= 0 else f'${MAX_LOSS_PER_TRADE:.0f}':12s}",
        f"║  MTF Filter: {'Yes' if MTF_FILTER else 'No':3s} | Root Filter: {'Yes' if ROOT_FILTER else 'No':3s} | Telegram: {'Yes' if TELEGRAM_BOT_TOKEN else 'No':3s}",
        "╚════════════════════════════════════════════════════════════════════╝",
    ]
    return "\n".join(lines)


# ============================================================================
# EXAMPLES OF .env FILE (FOR REFERENCE)
# ============================================================================

"""
EXAMPLE .env FILE:

# Network
MAINNET=true
USE_WS=false

# API Keys (KEEP THESE SAFE!)
BYBIT_API_KEY=your-api-key-here
BYBIT_API_SECRET=your-api-secret-here

# Timeframes
ROOT_TFS=60,240,D
MTF_TFS=5,15,60,240,D
ROOT_SCAN_INTERVAL=0

# Trading
TRADE_ENABLED=false
MAX_OPEN_TRADES=3

# Position Sizing
POSITION_SIZING_MODE=risk
RISK_PERCENT_PER_TRADE=2.0
MAX_LOSS_PER_TRADE=0

# Risk Management
TP_PERCENT=5.0
SL_PERCENT=2.0
BREAKEVEN_TRIGGER_PERCENT=3.0
BREAKEVEN_HL=entry

# Filtering
MTF_FILTER=true
ROOT_FILTER=false

# Notifications
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklmnoPQRstuvWXYZ-1234567890
TELEGRAM_CHAT_ID=987654321

# Logging
LOG_LEVEL=INFO
DEBUG_SURGICAL_LOGS=false
SEND_ALL_SIGNALS_ON_DEPLOY=true
"""
