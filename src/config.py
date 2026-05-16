import os
from dotenv import load_dotenv

load_dotenv()

def _csv_list(s: str):
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def _bool_env(v: str, default: bool) -> bool:
    s = os.getenv(v)
    if s is None:
        return default
    return s.lower() in ("1", "true", "yes", "y")

# Basic network / keys
MAINNET = _bool_env("MAINNET", True)
USE_WS = _bool_env("USE_WS", False)

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "") or ""
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "") or ""

# Symbol filters
EXCLUDE_STABLECOINS = _csv_list(os.getenv("EXCLUDE_STABLECOINS", "USDT,BUSD,USDC"))

# Timeframes
ROOT_TFS = _csv_list(os.getenv("ROOT_TFS", "1h,4h,1d"))
MTF_TFS = _csv_list(os.getenv("MTF_TFS", "5m,15m,1h,4h,1d"))

# Intervals and seed sizes
ROOT_SCAN_INTERVAL = int(os.getenv("ROOT_SCAN_INTERVAL", "300"))  # seconds
KLINE_SEED_LIMIT = int(os.getenv("KLINE_SEED_LIMIT", "200"))

# Concurrency / rate limiting
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "5"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

# Trading and risk
TRADE_ENABLED = _bool_env("TRADE_ENABLED", False)
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "3"))
TP_PERCENT = float(os.getenv("TP_PERCENT", "2.0"))
SL_PERCENT = float(os.getenv("SL_PERCENT", "1.0"))
BREAKEVEN_PERCENT = float(os.getenv("BREAKEVEN_PERCENT", "0.5"))
BREAKEVEN_TRIGGER_PERCENT = float(os.getenv("BREAKEVEN_TRIGGER_PERCENT", "1.0"))
BREAKEVEN_HL = _bool_env("BREAKEVEN_HL", True)

# Position sizing
POSITION_SIZING_MODE = os.getenv("POSITION_SIZING_MODE", "auto")  # 'auto' or 'fixed'
FIXED_QTY = float(os.getenv("FIXED_QTY", "1.0"))

# Scoring thresholds (used for scoring/prioritization; do not drop signals by default)
# MACD_HIST_THRESHOLD and VOLUME_CHANGE_24H_THRESHOLD are used in scoring when ROOT_FILTER=true.
MACD_HIST_THRESHOLD = float(os.getenv("MACD_HIST_THRESHOLD", "0.0"))          # histogram magnitude contributes to score
VOLUME_CHANGE_24H_THRESHOLD = float(os.getenv("VOLUME_CHANGE_24H_THRESHOLD", "0.0"))  # decimal (0.1 == +10%)

# Filter toggles (these do NOT drop signals; they change prioritization/acceptance rules)
# ROOT_FILTER:
#   - true  => prioritize signals per root TF and pick top ROOT_TOP_N per root TF (by score) for trade attempts.
#   - false => treat signals FIFO (first-come-first-served) when attempting opens (still limited by MAX_OPEN_TRADES).
ROOT_FILTER = _bool_env("ROOT_FILTER", False)
ROOT_TOP_N = int(os.getenv("ROOT_TOP_N", str(MAX_OPEN_TRADES)))  # how many top symbols per root TF to consider when ROOT_FILTER=true

# MTF_FILTER:
#   - true  => during MTF evaluation require that positive macd histograms are rising (cur > prev) to count strongly.
#             1d can be handled specially via MTF_1D_ALLOW_NEGATIVE_RISING.
#   - false => count positive histograms regardless of rising/falling for contribution to score.
MTF_FILTER = _bool_env("MTF_FILTER", False)
MTF_REQUIRE_RISING = _bool_env("MTF_REQUIRE_RISING", True)  # when MTF_FILTER==True, require rising positive hist to contribute
MTF_1D_ALLOW_NEGATIVE_RISING = _bool_env("MTF_1D_ALLOW_NEGATIVE_RISING", True)  # allow 1d negative but rising to be accepted

# MTF slope lookback for detecting 1d rising
MTF_SLOPE_LOOKBACK = int(os.getenv("MTF_SLOPE_LOOKBACK", "3"))

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") or ""

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
