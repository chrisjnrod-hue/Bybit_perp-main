# config.py
import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

def safe_int_env(name: str, default: int = 0) -> int:
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
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default

def safe_bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")

def safe_csv_list(name: str, default: Optional[List[str]] = None, sep: str = ",") -> List[str]:
    v = os.getenv(name)
    if v is None or v == "":
        return default or []
    try:
        return [x.strip() for x in v.split(sep) if x.strip()]
    except Exception:
        return default or []

# ---- Basic network / keys ----
MAINNET = safe_bool_env("MAINNET", True)
USE_WS = safe_bool_env("USE_WS", False)

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "") or ""
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "") or ""

# ---- Symbol filters ----
EXCLUDE_STABLECOINS = safe_csv_list("EXCLUDE_STABLECOINS", ["USDT", "BUSD", "USDC"])

# ---- Timeframes ----
# IMPORTANT: Use numeric intervals (in minutes) or "D" for daily, not "1h", "5m" format
# Bybit REST API expects: "5", "15", "60", "240", "D" etc.
ROOT_TFS = safe_csv_list("ROOT_TFS", ["60", "240", "D"])
MTF_TFS = safe_csv_list("MTF_TFS", ["5", "15", "60", "240", "D"])

# Intervals and seed sizes
# NOTE: If ROOT_SCAN_INTERVAL is 0 (default here) the scanner will run on 5m-candle opens.
ROOT_SCAN_INTERVAL = safe_int_env("ROOT_SCAN_INTERVAL", 0)  # seconds; 0 => run at each 5m candle open
KLINE_SEED_LIMIT = safe_int_env("KLINE_SEED_LIMIT", 200)

# Concurrency / rate limiting
RATE_LIMIT_RPS = safe_float_env("RATE_LIMIT_RPS", 5.0)
CONCURRENCY = safe_int_env("CONCURRENCY", 10)

# Trading and risk
TRADE_ENABLED = safe_bool_env("TRADE_ENABLED", False)
MAX_OPEN_TRADES = safe_int_env("MAX_OPEN_TRADES", 3)
TP_PERCENT = safe_float_env("TP_PERCENT", 2.0)
SL_PERCENT = safe_float_env("SL_PERCENT", 1.0)
BREAKEVEN_PERCENT = safe_float_env("BREAKEVEN_PERCENT", 0.5)
BREAKEVEN_TRIGGER_PERCENT = safe_float_env("BREAKEVEN_TRIGGER_PERCENT", 1.0)
BREAKEVEN_HL = safe_bool_env("BREAKEVEN_HL", True)

# Position sizing
POSITION_SIZING_MODE = os.getenv("POSITION_SIZING_MODE", "auto")
FIXED_QTY = safe_float_env("FIXED_QTY", 1.0)

# Scoring thresholds
MACD_HIST_THRESHOLD = safe_float_env("MACD_HIST_THRESHOLD", 0.0)
VOLUME_CHANGE_24H_THRESHOLD = safe_float_env("VOLUME_CHANGE_24H_THRESHOLD", 0.0)

# ---- 24h Volume change filter (trade-open gate only, never rejects signals) ----
# VOLUME_FILTER_ENABLED=true  → block trade opens when 24h vol change % is negative
# VOLUME_FILTER_ENABLED=false → ignore volume, open trades freely (default: true)
VOLUME_FILTER_ENABLED = safe_bool_env("VOLUME_FILTER_ENABLED", True)

# Minimum 24h volume change % required for a trade open (decimal; 0.0 = any positive change)
# e.g. VOLUME_MIN_CHANGE_PCT=0.05 requires +5 % volume growth before opening
VOLUME_MIN_CHANGE_PCT = safe_float_env("VOLUME_MIN_CHANGE_PCT", 0.0)

# Filters toggles
ROOT_FILTER = safe_bool_env("ROOT_FILTER", False)
ROOT_TOP_N = safe_int_env("ROOT_TOP_N", MAX_OPEN_TRADES)

MTF_FILTER = safe_bool_env("MTF_FILTER", False)
MTF_REQUIRE_RISING = safe_bool_env("MTF_REQUIRE_RISING", True)
MTF_1D_ALLOW_NEGATIVE_RISING = safe_bool_env("MTF_1D_ALLOW_NEGATIVE_RISING", True)

MTF_SLOPE_LOOKBACK = safe_int_env("MTF_SLOPE_LOOKBACK", 3)

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or ""

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
