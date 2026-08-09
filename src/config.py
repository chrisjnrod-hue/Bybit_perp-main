# config.py - COMPLETE UPDATED VERSION (with new environment variables)
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
# If ROOT_SCAN_INTERVAL is 0 (default) the scanner will run on 5m-candle opens.
ROOT_SCAN_INTERVAL = safe_int_env("ROOT_SCAN_INTERVAL", 0)  # seconds; 0 => run at each 5m candle open
KLINE_SEED_LIMIT = safe_int_env("KLINE_SEED_LIMIT", 50)

# Concurrency / rate limiting (optimized for faster scans)
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

# ---- Execution & Risk Filters (Leverage, Slippage, Spread) ----
LEVERAGE = safe_int_env("LEVERAGE", 10)
MAX_SLIPPAGE = safe_float_env("MAX_SLIPPAGE", 0.2)
MAX_SPREAD_PERCENT = safe_float_env("MAX_SPREAD_PERCENT", 0.1)

# Position sizing
POSITION_SIZING_MODE = os.getenv("POSITION_SIZING_MODE", "auto")
FIXED_QTY = safe_float_env("FIXED_QTY", 1.0)

# Scoring thresholds
MACD_HIST_THRESHOLD = safe_float_env("MACD_HIST_THRESHOLD", 0.0)
VOLUME_CHANGE_24H_THRESHOLD = safe_float_env("VOLUME_CHANGE_24H_THRESHOLD", 0.0)

# ---- 24h Volume / Volume-change filters (trade-open gate only, never rejects signals) ----
# VOLUME_FILTER_ENABLED=true  → apply volume-change gating for trade opens
# VOLUME_FILTER_ENABLED=false → ignore volume-change when opening trades (default: true)
VOLUME_FILTER_ENABLED = safe_bool_env("VOLUME_FILTER_ENABLED", True)

# Minimum 24h volume change fraction required for a trade open (decimal; 0.0 = any)
# e.g. VOLUME_MIN_CHANGE_PCT=0.05 requires +5% volume growth before opening
VOLUME_MIN_CHANGE_PCT = safe_float_env("VOLUME_MIN_CHANGE_PCT", 0.0)

# New: Minimum 24h USDT quote-volume required to allow opening trades. 0 disables the gate.
MIN_24H_VOLUME_USDT = safe_float_env("MIN_24H_VOLUME_USDT", 0.0)

# If True, block trade opens when 24h vol change is negative (even if VOLUME_FILTER_ENABLED is False)
TRADE_NO_NEG_VOL = safe_bool_env("TRADE_NO_NEG_VOL", True)

# Market cap filter (0 disables)
MARKET_CAP_MIN = safe_float_env("MARKET_CAP_MIN", 0.0)

# Filters toggles
ROOT_FILTER = safe_bool_env("ROOT_FILTER", False)
ROOT_TOP_N = safe_int_env("ROOT_TOP_N", MAX_OPEN_TRADES)

MTF_FILTER = safe_bool_env("MTF_FILTER", False)
MTF_REQUIRE_RISING = safe_bool_env("MTF_REQUIRE_RISING", True)
MTF_1D_ALLOW_NEGATIVE_RISING = safe_bool_env("MTF_1D_ALLOW_NEGATIVE_RISING", True)

MTF_SLOPE_LOOKBACK = safe_int_env("MTF_SLOPE_LOOKBACK", 3)

# ---- MACD Flip & Candle Age Filtering ----
# Prevent mid-candle flips from opening trades.
FLIP_CANDLE_AGE_MAX_SEC = safe_int_env("FLIP_CANDLE_AGE_MAX_SEC", 300)

# Signal deduplication window (seconds)
SIGNAL_DEDUP_WINDOW = safe_int_env("SIGNAL_DEDUP_WINDOW", 60)

# ---- TV Rating / prioritization ----
# Minimum TV rating score required to open a trade
TRADE_RATING_MIN = safe_float_env("TRADE_RATING_MIN", 0.25)

# When True, sort candidates by TV rating (highest first) during slot allocation
TRADE_RATING_PRIORITIZE = safe_bool_env("TRADE_RATING_PRIORITIZE", True)

# TV rating weight used in combined score calculation (0.0..1.0), where 1.0 = 100% TV weight
TV_RATING_WEIGHT = safe_float_env("TV_RATING_WEIGHT", 0.3)

# Priority order for filling slots by timeframe (comma-separated list, e.g. "240,D,60")
PRIORITIZE_SLOT_ORDER = safe_csv_list("PRIORITIZE_SLOT_ORDER", ["240", "D", "60"])

# ---- Telegram ----
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or ""

# ---- Logging ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# -------------------------
# TradingView-like Technical Rating config (local implementation)
# -------------------------
TECHNICAL_RATING = {
    "enabled": True,
    "ta_backend": "pandas_ta",  # "pandas_ta" recommended (pure Python)
    "timeframes": ["60", "240", "D"],  # root TFs to include in aggregate rating
    "timeframe_weights": {"60": 0.5, "240": 0.8, "D": 1.0},
    "indicators": {
        "ma_pairs": [[10, 50], [20, 100], [50, 200]],
        "rsi_period": 14,
        "macd": [12, 26, 9],
        "stochastic": [14, 3, 3],
        "adx_period": 14,
        "bollinger": [20, 2],
        "cci_period": 20,
        "willr_period": 14,
    },
    "weights": {
        "ma_pair": 1.0,
        "macd": 1.5,
        "rsi": 1.0,
        "stochastic": 0.8,
        "obv": 1.0,
        "bollinger": 0.6,
        "cci": 0.6,
        "willr": 0.4,
    },
    "tolerance": {
        "ma_pair_pct": 0.002,
        "obv_slope_lookback": 5,
    },
    "adx": {"threshold": 25, "multiplier": 1.25},
    "thresholds": {
        "strong_buy": 0.6,
        "buy": 0.25,
        "sell": -0.25,
        "strong_sell": -0.6
    },
    "multi_timeframe_aggregation": "weighted",
    "benchmarking": {
        "fetch_tradingview_labels": False,
        "tradingview_rate_limit_sleep": 2
    }
}

# Backwards-compatible alias (some modules expect TRADE_RATING_MIN_VAL)
TRADE_RATING_MIN_VAL = TRADE_RATING_MIN

# Export list of keys
__all__ = [
    "MAINNET", "USE_WS", "BYBIT_API_KEY", "BYBIT_API_SECRET",
    "EXCLUDE_STABLECOINS",
    "ROOT_TFS", "MTF_TFS", "ROOT_SCAN_INTERVAL", "KLINE_SEED_LIMIT",
    "RATE_LIMIT_RPS", "CONCURRENCY", "MAX_CONCURRENT_REQUESTS", "REQUEST_BATCH_SIZE", "REQUEST_BATCH_DELAY", "REST_POLL_INTERVAL",
    "TRADE_ENABLED", "MAX_OPEN_TRADES", "TP_PERCENT", "SL_PERCENT",
    "BREAKEVEN_PERCENT", "BREAKEVEN_TRIGGER_PERCENT", "BREAKEVEN_HL",
    "LEVERAGE", "MAX_SLIPPAGE", "MAX_SPREAD_PERCENT",
    "POSITION_SIZING_MODE", "FIXED_QTY",
    "MACD_HIST_THRESHOLD", "VOLUME_CHANGE_24H_THRESHOLD",
    "VOLUME_FILTER_ENABLED", "VOLUME_MIN_CHANGE_PCT", "MIN_24H_VOLUME_USDT", "TRADE_NO_NEG_VOL", "MARKET_CAP_MIN",
    "ROOT_FILTER", "ROOT_TOP_N", "MTF_FILTER", "MTF_REQUIRE_RISING", "MTF_1D_ALLOW_NEGATIVE_RISING", "MTF_SLOPE_LOOKBACK",
    "FLIP_CANDLE_AGE_MAX_SEC", "SIGNAL_DEDUP_WINDOW",
    "TRADE_RATING_MIN", "TRADE_RATING_PRIORITIZE", "TV_RATING_WEIGHT", "PRIORITIZE_SLOT_ORDER",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "LOG_LEVEL",
    "TECHNICAL_RATING"
]
