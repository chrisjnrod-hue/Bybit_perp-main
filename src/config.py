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
ROOT_TFS = safe_csv_list("ROOT_TFS", ["60", "240", "D"])
MTF_TFS = safe_csv_list("MTF_TFS", ["5", "15", "60", "240", "D"])

# Intervals and seed sizes
_raw_root_scan = os.getenv("ROOT_SCAN_INTERVAL", "")
if isinstance(_raw_root_scan, str) and _raw_root_scan.strip().lower() in ("false", "off", "0", "none", ""):
    ROOT_SCAN_INTERVAL = 0
else:
    ROOT_SCAN_INTERVAL = safe_int_env("ROOT_SCAN_INTERVAL", 120)

KLINE_SEED_LIMIT = safe_int_env("KLINE_SEED_LIMIT", 50)

# Concurrency / rate limiting
RATE_LIMIT_RPS = safe_float_env("RATE_LIMIT_RPS", 5.0)
CONCURRENCY = safe_int_env("CONCURRENCY", 30)
MAX_CONCURRENT_REQUESTS = safe_int_env("MAX_CONCURRENT_REQUESTS", 10)
REQUEST_BATCH_SIZE = safe_int_env("REQUEST_BATCH_SIZE", 15)
REQUEST_BATCH_DELAY = safe_float_env("REQUEST_BATCH_DELAY", 0.2)
REST_POLL_INTERVAL = safe_int_env("REST_POLL_INTERVAL", 10)

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

# 24h Volume change filter (trade-open gate only)
VOLUME_FILTER_ENABLED = safe_bool_env("VOLUME_FILTER_ENABLED", True)
VOLUME_MIN_CHANGE_PCT = safe_float_env("VOLUME_MIN_CHANGE_PCT", 0.0)

# Filters toggles (keep MACD, volume, SR)
SIGNAL_FILTER_MACD_ENABLED = safe_bool_env("SIGNAL_FILTER_MACD_ENABLED", True)
SIGNAL_FILTER_VOLUME_ENABLED = safe_bool_env("SIGNAL_FILTER_VOLUME_ENABLED", True)
SIGNAL_FILTER_SR_ENABLED = safe_bool_env("SIGNAL_FILTER_SR_ENABLED", True)

SIGNAL_WEIGHT_MACD = safe_float_env("SIGNAL_WEIGHT_MACD", 1.0)
SIGNAL_WEIGHT_VOLUME = safe_float_env("SIGNAL_WEIGHT_VOLUME", 1.0)
SIGNAL_WEIGHT_SR = safe_float_env("SIGNAL_WEIGHT_SR", 0.5)

SIGNAL_SR_SUPPORT_WINDOW_PCT = safe_float_env("SIGNAL_SR_SUPPORT_WINDOW_PCT", 0.02)
SIGNAL_SR_LOOKBACK = safe_int_env("SIGNAL_SR_LOOKBACK", 100)

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or ""

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------------
# StrongBuy / Technical rating config (new)
# ---------------------
STRONGBUY_MIN_SCORE = safe_float_env("STRONGBUY_MIN_SCORE", 0.75)       # normalized composite [0..1]
STRONGBUY_MODERATE_SCORE = safe_float_env("STRONGBUY_MODERATE_SCORE", 0.60)
STRONGBUY_REQUIRED_FOR_OPEN = safe_bool_env("STRONGBUY_REQUIRED_FOR_OPEN", True)

# Indicator params
RSI_PERIOD = safe_int_env("RSI_PERIOD", 14)
EMA_SHORT_PERIOD = safe_int_env("EMA_SHORT_PERIOD", 20)
EMA_LONG_PERIOD = safe_int_env("EMA_LONG_PERIOD", 50)

# Normalization scales (used in tanh() denominators)
MACD_NORM_SCALE = safe_float_env("MACD_NORM_SCALE", 0.0005)
EMA_NORM_SCALE = safe_float_env("EMA_NORM_SCALE", 0.01)
VOL_NORM_SCALE = safe_float_env("VOL_NORM_SCALE", 0.05)

# Indicator weights (sum will be normalized internally)
INDICATOR_WEIGHT_MACD = safe_float_env("INDICATOR_WEIGHT_MACD", 0.30)
INDICATOR_WEIGHT_RSI = safe_float_env("INDICATOR_WEIGHT_RSI", 0.25)
INDICATOR_WEIGHT_EMA = safe_float_env("INDICATOR_WEIGHT_EMA", 0.25)
INDICATOR_WEIGHT_VOL = safe_float_env("INDICATOR_WEIGHT_VOL", 0.20)

# Sent signal cache TTL (seconds) to avoid duplicate telegram posts for same candle
SENT_SIGNAL_TTL = safe_int_env("SENT_SIGNAL_TTL", 60 * 60 * 4)
