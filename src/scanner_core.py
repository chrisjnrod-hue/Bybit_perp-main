"""
scanner_core.py

Pure, synchronous helpers and deterministic computations extracted from scanner.py.

These functions:
- Do NOT perform network I/O
- Accept inputs (klines, closes, volume dicts, config) to be unit-testable
- Preserve behavior of original helpers (normalization, MACD wrapper, TV rating, quantize, MTF alignment)
"""
import math
import json
import statistics
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Callable, Any as AnyT

getcontext().prec = 28

# pandas/ta optional
try:
    import pandas as pd
    import numpy as np
except Exception:
    pd = None  # type: ignore
    np = None  # type: ignore

# Try to import pandas_ta first (preferred API: ta.sma, ta.macd, etc.)
_PANDAS_TA_AVAILABLE = False
_PANDAS_TA_STYLE = False
_ta_module = None
try:
    import pandas_ta as ta  # type: ignore
    _ta_module = ta
    _PANDAS_TA_AVAILABLE = True
    _PANDAS_TA_STYLE = True
except Exception:
    try:
        import ta  # type: ignore
        _ta_module = ta
        _PANDAS_TA_AVAILABLE = True
        _PANDAS_TA_STYLE = False
    except Exception:
        ta = None  # type: ignore
        _ta_module = None
        _PANDAS_TA_AVAILABLE = False
        _PANDAS_TA_STYLE = False

# Import MACD helper and slope function from package (keeps same external dependency)
# If you have a macd.py with macd_histogram and slope, keep it; otherwise fallback implementations below will handle MACD.
try:
    from .macd import macd_histogram, slope  # type: ignore
except Exception:
    macd_histogram = None
    slope = None  # type: ignore

# --------------------
# Timeframe helpers
# --------------------
def tf_to_seconds(tf: str) -> int:
    try:
        s = str(tf)
        if s.endswith("m"):
            return int(s[:-1]) * 60
        if s.endswith("h"):
            return int(s[:-1]) * 3600
        if s == "D" or s.endswith("d"):
            try:
                if s == "D":
                    return 24 * 3600
                return int(s[:-1]) * 86400
            except Exception:
                return 24 * 3600
        return int(s) * 60
    except Exception:
        return 60


def is_candle_age_acceptable(start_at: Optional[int], now: float, max_age_sec: int) -> bool:
    """
    Check if the candle's start time is fresh enough compared to current time.
    Behavior:
    - If max_age_sec <= 0: check is disabled -> return True.
    - If start_at is None/unparseable: return True.
    - start_at may be seconds or milliseconds; detect and normalize.
    """
    try:
        if max_age_sec is None or int(max_age_sec) <= 0:
            return True
        if start_at is None:
            return True
        # detect milliseconds
        start_sec = float(start_at) / 1000.0 if int(start_at) > 10000000000 else float(start_at)
        age = now - start_sec
        return age <= float(max_age_sec)
    except Exception:
        return True


# --------------------
# Normalization helpers
# --------------------
def normalize_klines(raw_klines: AnyT, tf: str) -> List[Dict[str, AnyT]]:
    """
    Normalize various kline shapes into list of dicts:
      {"start_at", "open", "high", "low", "close", "volume", "is_closed"(optional)}
    Accepts dicts, lists, tuples and attempts to extract common fields.
    """
    out: List[Dict[str, AnyT]] = []
    if not raw_klines:
        return out

    if isinstance(raw_klines, dict):
        if "list" in raw_klines and isinstance(raw_klines["list"], (list, dict)):
            raw_klines = raw_klines["list"]
        elif "result" in raw_klines and isinstance(raw_klines["result"], (list, dict)):
            raw_klines = raw_klines["result"]
        elif "data" in raw_klines and isinstance(raw_klines["data"], (list, dict)):
            raw_klines = raw_klines["data"]

    if not isinstance(raw_klines, (list, tuple)):
        if isinstance(raw_klines, dict):
            seq = [raw_klines]
        else:
            seq = [raw_klines] if raw_klines else []
    else:
        seq = raw_klines

    for item in seq:
        try:
            if isinstance(item, (list, tuple)):
                start = None
                open_p = None
                high = None
                low = None
                close = None
                vol = None
                if len(item) >= 1:
                    try:
                        start = int(item[0])
                    except Exception:
                        start = None
                if len(item) >= 2:
                    try:
                        open_p = float(item[1])
                    except Exception:
                        open_p = None
                if len(item) >= 3:
                    try:
                        high = float(item[2])
                    except Exception:
                        high = None
                if len(item) >= 4:
                    try:
                        low = float(item[3])
                    except Exception:
                        low = None
                if len(item) >= 5:
                    try:
                        close = float(item[4])
                    except Exception:
                        close = None
                if len(item) >= 6:
                    try:
                        vol = float(item[5])
                    except Exception:
                        vol = None

                if close is not None:
                    out.append({
                        "start_at": start, "open": open_p, "high": high, "low": low,
                        "close": close, "volume": vol
                    })
                continue

            if isinstance(item, dict):
                start = (
                    item.get("start_at")
                    or item.get("open_time")
                    or item.get("t")
                    or item.get("timestamp")
                    or item.get("start")
                    or item.get("time")
                )
                open_p = (
                    item.get("open")
                    or item.get("openPrice")
                    or item.get("o")
                )
                high = (
                    item.get("high")
                    or item.get("h")
                )
                low = (
                    item.get("low")
                    or item.get("l")
                )
                close = (
                    item.get("close")
                    or item.get("close_price")
                    or item.get("c")
                    or item.get("last_price")
                    or item.get("Close")
                )
                vol = (
                    item.get("volume")
                    or item.get("vol")
                    or item.get("turnover")
                    or item.get("v")
                    or item.get("quoteAsset")
                )
                is_closed = item.get("isClosed")
                if is_closed is None:
                    is_closed = item.get("is_closed") or item.get("complete") or item.get("confirmed")

                try:
                    if start is not None:
                        start = int(start)
                except Exception:
                    start = None
                try:
                    if open_p is not None:
                        open_p = float(open_p)
                except Exception:
                    open_p = None
                try:
                    if high is not None:
                        high = float(high)
                except Exception:
                    high = None
                try:
                    if low is not None:
                        low = float(low)
                except Exception:
                    low = None
                try:
                    if close is not None:
                        close = float(close)
                except Exception:
                    close = None
                try:
                    if vol is not None:
                        vol = float(vol)
                except Exception:
                    vol = None

                if close is not None:
                    out.append({
                        "start_at": start, "open": open_p, "high": high, "low": low,
                        "close": close, "volume": vol, "is_closed": is_closed
                    })
                continue
        except Exception:
            continue

    return out


# --------------------
# Quantize helper
# --------------------
def quantize_qty(qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
    if qty is None:
        return 0.0
    qty_d = Decimal(str(qty))
    if step is None or step <= 0:
        if min_qty and qty_d < Decimal(str(min_qty)):
            return float(Decimal(str(min_qty)))
        return float(qty_d)
    step_d = Decimal(str(step))
    mult = (qty_d / step_d).to_integral_value(rounding=ROUND_DOWN)
    quant = (mult * step_d)
    if min_qty is not None:
        min_d = Decimal(str(min_qty))
        if quant < min_d:
            quant = min_d
    try:
        quant = quant.normalize()
    except Exception:
        pass
    return float(quant)


# --------------------
# MACD computation (ROBUST & ENHANCED)
# --------------------
def _is_finite_number(x: Any) -> bool:
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False


def _clean_hist(hist: Any) -> List[float]:
    out: List[float] = []
    if hist is None:
        return out
    try:
        if hasattr(hist, "tolist"):
            iterable = hist.tolist()
        else:
            iterable = list(hist) if not isinstance(hist, (str, bytes)) else [hist]
    except Exception:
        try:
            iterable = list(hist)
        except Exception:
            iterable = [hist]
    for x in iterable:
        try:
            if x is None:
                continue
            v = float(x)
            if math.isnan(v) or not math.isfinite(v):
                continue
            out.append(v)
        except Exception:
            continue
    return out


def _fallback_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    """
    Pure-Python MACD calculation using EMA.
    Robust implementation with better handling of edge cases.
    """
    if not closes:
        return [], [], []
    
    def ema(values: List[float], period: int) -> List[float]:
        """Calculate EMA with proper initialization."""
        if not values or period < 1:
            return []
        
        out: List[float] = []
        alpha = 2.0 / (period + 1.0)
        
        # Initialize with SMA of first 'period' values
        if len(values) < period:
            # If we have fewer values than period, use simple average
            sma_val = sum(values) / len(values)
            for v in values:
                out.append(sma_val)
            return out
        
        # Calculate initial SMA
        sma_val = sum(values[:period]) / period
        out.append(sma_val)
        
        # EMA for remaining values
        for v in values[period:]:
            sma_val = (float(v) * alpha) + (sma_val * (1.0 - alpha))
            out.append(sma_val)
        
        return out
    
    try:
        # Calculate EMAs
        fast_ema = ema(closes, fast)
        slow_ema = ema(closes, slow)
        
        if not fast_ema or not slow_ema or len(fast_ema) < slow or len(slow_ema) < slow:
            print(f"[MACD_DEBUG] EMA calculation failed: fast_len={len(fast_ema)}, slow_len={len(slow_ema)}, need>={slow}")
            return [], [], []
        
        # Calculate MACD line
        macd_series = [f - s for f, s in zip(fast_ema, slow_ema)]
        
        # Calculate Signal line (EMA of MACD)
        signal_series = ema(macd_series, signal)
        
        if not signal_series:
            print(f"[MACD_DEBUG] Signal line calculation failed")
            return [], [], []
        
        # Calculate Histogram
        hist_series = [m - s for m, s in zip(macd_series, signal_series)]
        
        print(f"[MACD_DEBUG] Fallback MACD calculated: closes={len(closes)}, macd={len(macd_series)}, signal={len(signal_series)}, hist={len(hist_series)}")
        if hist_series:
            print(f"[MACD_DEBUG] Last 3 histogram values: {hist_series[-3:] if len(hist_series) >= 3 else hist_series}")
        
        return macd_series, signal_series, hist_series
    except Exception as e:
        print(f"[MACD_ERROR] Fallback MACD failed: {e}")
        return [], [], []


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Returns (macd_last, signal_last, hist_list) where hist_list is oldest->newest plain list.
    
    ENHANCED: Better validation, logging, and fallback handling.
    """
    # ============ ENHANCED: Input validation ============
    data: List[float] = []
    for c in closes:
        try:
            if c is None:
                continue
            val = float(c)
            if math.isfinite(val):  # Filter NaN/inf
                data.append(val)
        except Exception:
            continue
    
    print(f"[MACD_INPUT] Input closes: {len(closes)} raw, {len(data)} valid finite values")
    
    # ============ ENHANCED: Minimum window check ============
    min_window = slow + signal  # 26 + 9 = 35 minimum
    # Previously code returned early if len(data) < min_window. That prevented fallback from running.
    # Let the fallback attempt computation even with fewer-than-recommended samples; warn accordingly.
    if len(data) < slow:
        print(f"[MACD_UNDERFLOW] WARNING: {len(data)} closes < slow ({slow}). Will attempt MACD fallback; results may be noisy.")
    elif len(data) < min_window:
        print(f"[MACD_INFO] Only {len(data)} closes (< recommended {min_window}) available; will attempt fallback MACD computation.")

    if include_price is not None:
        try:
            current_price = float(include_price)
            if math.isfinite(current_price):
                # Replace last close with current price for real-time calculation
                if data:
                    data[-1] = current_price
                    print(f"[MACD_INPUT] Included current price: {current_price}")
                else:
                    # no closes present; include current price as single value
                    data.append(current_price)
                    print(f"[MACD_INPUT] No historical closes; using current price as single data point: {current_price}")
        except Exception:
            pass

    # ============ Try external macd_histogram helper first ============
    if macd_histogram is not None:
        try:
            print(f"[MACD_ATTEMPT] Trying external macd_histogram helper with {len(data)} closes...")
            macd_line_raw, signal_line_raw, hist_raw = macd_histogram(data)
            hist_list = _clean_hist(hist_raw)
            
            macd_last = None
            signal_last = None
            
            try:
                if hasattr(macd_line_raw, "__len__") and len(macd_line_raw):
                    macd_last = float(macd_line_raw[-1])
            except Exception:
                pass
            
            try:
                if hasattr(signal_line_raw, "__len__") and len(signal_line_raw):
                    signal_last = float(signal_line_raw[-1])
            except Exception:
                pass
            
            if hist_list:
                print(f"[MACD_SUCCESS] External helper returned {len(hist_list)} histogram values")
                print(f"[MACD_SUCCESS] Last 3 histogram: {hist_list[-3:] if len(hist_list) >= 3 else hist_list}")
                return macd_last, signal_last, hist_list
            else:
                print(f"[MACD_FAIL] External helper returned empty histogram, falling back to pure Python")
        except Exception as e:
            print(f"[MACD_FAIL] External helper failed: {e}, falling back to pure Python")

    # ============ Fallback to pure-python MACD ============
    print(f"[MACD_FALLBACK] Using pure-Python MACD fallback with {len(data)} closes")
    macd_series, signal_series, hist_series = _fallback_macd(data, fast=fast, slow=slow, signal=signal)
    
    if not hist_series:
        print(f"[MACD_FAILURE] Pure-Python MACD also failed - returning empty")
        return None, None, []
    
    macd_last = float(macd_series[-1]) if macd_series else None
    signal_last = float(signal_series[-1]) if signal_series else None
    
    print(f"[MACD_RESULT] Fallback MACD computed: hist_len={len(hist_series)}, macd_last={macd_last}, signal_last={signal_last}")
    if hist_series:
        print(f"[MACD_RESULT] Last 3 histogram: {hist_series[-3:] if len(hist_series) >= 3 else hist_series}")
    
    return macd_last, signal_last, hist_series


def _safe_last(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if hasattr(v, "iloc"):
            if len(v) == 0:
                return None
            val = v.iloc[-1]
            return float(val) if _is_finite_number(val) else None
        if hasattr(v, "tolist"):
            lst = v.tolist()
            if not lst:
                return None
            return float(lst[-1]) if _is_finite_number(lst[-1]) else None
        if isinstance(v, (list, tuple)):
            if not v:
                return None
            return float(v[-1]) if _is_finite_number(v[-1]) else None
        return float(v) if _is_finite_number(v) else None
    except Exception:
        return None


# --------------------
# Flip detection (ROBUST)
# --------------------
def detect_flip_current_open(hist: List[float], hist_threshold: Optional[float] = None, std_mult: float = 0.05, abs_min: float = 1e-9, lookback: int = 1) -> bool:
    """
    Detect zero-cross flip from negative (or <=0) to positive on last candle, with noise gating.
    ENHANCED: Better logging and threshold handling.
    """
    try:
        clean = _clean_hist(hist)
        
        if not clean:
            print(f"[FLIP_DEBUG] Empty histogram after cleaning")
            return False
        
        if len(clean) < (lookback + 1):
            print(f"[FLIP_DEBUG] Insufficient history: {len(clean)} < {lookback + 1}")
            return False
        
        prev = clean[-(lookback + 1)]
        cur = clean[-1]
        
        if prev is None or cur is None:
            print(f"[FLIP_DEBUG] None values: prev={prev}, cur={cur}")
            return False
        
        # Calculate threshold
        if hist_threshold is None:
            try:
                hist_std = statistics.pstdev(clean) if len(clean) >= 2 else 0.0
            except Exception:
                hist_std = 0.0
            
            hist_threshold = max(abs_min, abs(hist_std) * std_mult)
            hist_threshold = min(hist_threshold, 0.01)  # Cap at 1%
        
        # Detect flip: prev negative/zero, cur positive above threshold
        flip = (prev < -1e-9) and (cur > hist_threshold)
        
        print(f"[FLIP_DEBUG] prev={prev:.8f}, cur={cur:.8f}, threshold={hist_threshold:.8f}, flip={flip}")
        
        return flip
    except Exception as e:
        print(f"[FLIP_ERROR] {e}")
        return False


# --------------------
# Volume helper (unchanged)
# --------------------
def compute_24h_volume_change_from(vol_data: Optional[Dict[str, float]]) -> Optional[float]:
    try:
        if not vol_data:
            print("[VOL_DEBUG] compute_24h_volume_change: no vol_data provided")
            return None
        prev_vol = vol_data.get("previous", 0)
        curr_vol = vol_data.get("current", 0)
        if prev_vol <= 0:
            print(f"[VOL_DEBUG] compute_24h_volume_change: prev_vol <= 0 (prev={prev_vol}, curr={curr_vol})")
            return None
        change = (curr_vol - prev_vol) / prev_vol
        result = min(change, 1.0)
        try:
            print(f"[VOL_DEBUG] prev={prev_vol:.0f}, curr={curr_vol:.0f}, change={change:.4f} => result_clamped={result:.4f}")
        except Exception:
            print(f"[VOL_DEBUG] prev={prev_vol}, curr={curr_vol}, change={change}")
        return result
    except Exception as e:
        print(f"[VOL_DEBUG] compute_24h_volume_change error: {e}")
        return None


# --------------------
# Indicator wrappers (unchanged)
# --------------------
def _safe_sma(df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.sma(df_close, length=length)
        else:
            return _ta_module.trend.SMAIndicator(df_close, window=int(length)).sma_indicator()
    except Exception:
        return None


def _safe_macd(df_close, fast: int, slow: int, signal: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.macd(df_close, fast=fast, slow=slow, signal=signal)
        else:
            macd_obj = _ta_module.trend.MACD(df_close, window_slow=int(slow), window_fast=int(fast), window_sign=int(signal))
            try:
                import pandas as _pd
                df_macd = _pd.DataFrame({
                    "MACD": macd_obj.macd(),
                    "MACD_signal": macd_obj.macd_signal(),
                    "MACD_hist": macd_obj.macd_diff()
                }, index=df_close.index)
                return df_macd
            except Exception:
                return None
    except Exception:
        return None


def _safe_rsi(df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.rsi(df_close, length=length)
        else:
            return _ta_module.momentum.RSIIndicator(df_close, window=int(length)).rsi()
    except Exception:
        return None


def _safe_stoch(df_high, df_low, df_close, k: int, d: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.stoch(high=df_high, low=df_low, close=df_close, k=k, d=d)
        else:
            stoch_obj = _ta_module.momentum.StochasticOscillator(high=df_high, low=df_low, close=df_close, window=int(k), smooth_window=int(d))
            try:
                import pandas as _pd
                df_st = _pd.DataFrame({
                    "STOCHk": stoch_obj.stoch(),
                    "STOCHd": stoch_obj.stoch_signal()
                }, index=df_close.index)
                return df_st
            except Exception:
                return None
    except Exception:
        return None


def _safe_adx(df_high, df_low, df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.adx(high=df_high, low=df_low, close=df_close, length=length)
        else:
            adx_obj = _ta_module.trend.ADXIndicator(high=df_high, low=df_low, close=df_close, window=int(length))
            try:
                import pandas as _pd
                df_adx = _pd.DataFrame({
                    "ADX": adx_obj.adx()
                }, index=df_close.index)
                return df_adx
            except Exception:
                return None
    except Exception:
        return None


def _safe_obv(df_close, df_volume):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.obv(df_close, df_volume)
        else:
            return _ta_module.volume.OnBalanceVolumeIndicator(close=df_close, volume=df_volume).on_balance_volume()
    except Exception:
        return None


def _safe_bbands(df_close, length: int, std: float):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.bbands(df_close, length=length, std=std)
        else:
            bb = _ta_module.volatility.BollingerBands(close=df_close, window=int(length), window_dev=float(std))
            try:
                import pandas as _pd
                df_bb = _pd.DataFrame({
                    "BB_bbm": bb.bollinger_mavg(),
                    "BB_bbh": bb.bollinger_hband(),
                    "BB_bbl": bb.bollinger_lband()
                }, index=df_close.index)
                return df_bb
            except Exception:
                return None
    except Exception:
        return None


# --------------------
# TV rating & MTF alignment
# --------------------
def compute_tv_rating_from(klines: List[Dict[str, AnyT]], cfg: Dict[str, AnyT], tf: Optional[str] = None, price: Optional[float] = None) -> Tuple[float, str]:
    # Keep original logic but robustly handle missing pandas/pandas_ta
    try:
        print(f"[TV_DEBUG] compute_tv_rating start tf={tf} price={price} candles={len(klines) if klines is not None else 0} pandas_ta_available={_PANDAS_TA_AVAILABLE} pandas_ta_style={_PANDAS_TA_STYLE}")
    except Exception:
        pass

    if not cfg.get("enabled", True):
        print("[TV_DEBUG] TECHNICAL_RATING disabled in config -> Neutral")
        return 0.0, "Neutral"

    if not _PANDAS_TA_AVAILABLE or pd is None or np is None:
        print("[TV_DEBUG] pandas/pandas_ta/ta not available -> returning Neutral")
        return 0.0, "Neutral"

    try:
        ma_max = max([n for pair in cfg["indicators"]["ma_pairs"] for n in pair]) if cfg["indicators"].get("ma_pairs") else 50
        min_candles = max(26, ma_max + 5)

        if not klines or len(klines) < min_candles:
            print(f"[TV_DEBUG] insufficient candles for tf={tf}: have={len(klines) if klines else 0} need={min_candles}")
            return 0.0, "Neutral"

        df = pd.DataFrame(klines)
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns:
                df[col] = np.nan

        df = df.dropna(subset=["close"]).copy()
        try:
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["high"] = pd.to_numeric(df["high"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
        except Exception as e:
            print(f"[TV_DEBUG] dtype conversion error for tf={tf}: {e}")
            return 0.0, "Neutral"

        if price is not None:
            try:
                df.at[df.index[-1], "close"] = float(price)
            except Exception:
                pass

        try:
            ma_lengths = sorted(set([n for pair in cfg["indicators"]["ma_pairs"] for n in pair]))
            for l in ma_lengths:
                sma_series = _safe_sma(df["close"], length=int(l))
                if sma_series is not None:
                    df[f"sma_{l}"] = sma_series
            print(f"[TV_DEBUG] computed SMAs: {ma_lengths}")

            macd_cfg = cfg["indicators"].get("macd", [12, 26, 9])
            macd_result = _safe_macd(df["close"], fast=int(macd_cfg[0]), slow=int(macd_cfg[1]), signal=int(macd_cfg[2]))
            if macd_result is not None and hasattr(macd_result, "columns") and len(macd_result.columns) > 0:
                macd_hist_col = next((c for c in macd_result.columns if "MACD" in str(c).upper() and ("H" in str(c).upper() or "diff" in str(c).lower())), None)
                if macd_hist_col:
                    df["macd_hist"] = macd_result[macd_hist_col]
                else:
                    df["macd_hist"] = macd_result.iloc[:, -1] if len(macd_result.columns) > 0 else 0.0
            else:
                df["macd_hist"] = 0.0
            print(f"[TV_DEBUG] computed MACD (macd_result columns: {list(macd_result.columns) if macd_result is not None and hasattr(macd_result,'columns') else 'n/a'})")

            rsi_series = _safe_rsi(df["close"], length=int(cfg["indicators"].get("rsi_period", 14)))
            df["rsi"] = rsi_series
            print("[TV_DEBUG] computed RSI")

            stoch_cfg = cfg["indicators"].get("stochastic", [14, 3, 3])
            try:
                st = _safe_stoch(df["high"], df["low"], df["close"], k=int(stoch_cfg[0]), d=int(stoch_cfg[1]))
                if st is not None and hasattr(st, "columns"):
                    stoch_k_col = next((c for c in st.columns if "STOCH" in str(c).upper() or "k" in str(c).lower()), None)
                    if stoch_k_col:
                        df["stoch_k"] = st[stoch_k_col]
                print("[TV_DEBUG] computed Stochastic (k)")
            except Exception:
                print("[TV_DEBUG] stoch calc failed (continuing)")

            try:
                adx_result = _safe_adx(df["high"], df["low"], df["close"], length=int(cfg["indicators"].get("adx_period", 14)))
                if adx_result is not None and hasattr(adx_result, "columns"):
                    adx_col = next((c for c in adx_result.columns if "ADX" in str(c).upper()), None)
                    if adx_col:
                        df["adx"] = adx_result[adx_col]
                    else:
                        df["adx"] = np.nan
                else:
                    df["adx"] = np.nan
                print("[TV_DEBUG] computed ADX")
            except Exception:
                df["adx"] = np.nan
                print("[TV_DEBUG] adx calc failed (continuing)")

            obv_series = _safe_obv(df["close"], df["volume"])
            df["obv"] = obv_series
            try:
                print(f"[TV_DEBUG] computed OBV (length={len(df['obv'].dropna())})")
            except Exception:
                print("[TV_DEBUG] computed OBV")

            try:
                boll_cfg = cfg["indicators"].get("bollinger", [20, 2])
                bb = _safe_bbands(df["close"], length=int(boll_cfg[0]), std=float(boll_cfg[1]))
                if bb is not None and hasattr(bb, "columns"):
                    for c in bb.columns:
                        df[c] = bb[c]
                print("[TV_DEBUG] computed Bollinger Bands")
            except Exception:
                print("[TV_DEBUG] bb calc failed (continuing)")

        except Exception as e:
            print(f"[TV_DEBUG] indicator computation error for tf={tf}: {e}")
            return 0.0, "Neutral"

        # scoring (same as original logic)
        last = df.iloc[-1]
        scores: List[Tuple[float, float]] = []
        weights = cfg.get("weights", {})

        for pair in cfg["indicators"]["ma_pairs"]:
            short, long = pair
            s = last.get(f"sma_{short}")
            l = last.get(f"sma_{long}")
            if pd.isna(s) or pd.isna(l) or l == 0:
                continue
            pct = (s - l) / l
            tol = cfg["tolerance"].get("ma_pair_pct", 0.002)
            if abs(pct) <= tol:
                sub = 0.0
            else:
                mag = max(-1.0, min(1.0, pct / 0.02))
                sub = float(np.tanh(mag * 2.0))
            scores.append((sub, weights.get("ma_pair", 1.0)))

        macd_hist = last.get("macd_hist")
        if macd_hist is not None and not pd.isna(macd_hist):
            hist_series = df["macd_hist"].dropna()
            denom = hist_series.std() if len(hist_series) > 0 else 1.0
            denom = denom if denom != 0 else 1.0
            sub = float(np.tanh((macd_hist / denom)))
            scores.append((sub, weights.get("macd", 1.5)))

        rsi = last.get("rsi")
        if rsi is not None and not pd.isna(rsi):
            sub = float((rsi - 50.0) / 50.0)
            sub = max(-1.0, min(1.0, sub))
            scores.append((sub, weights.get("rsi", 1.0)))

        k = last.get("stoch_k")
        if k is not None and not pd.isna(k):
            sub = 0.0
            if k > 80:
                sub = (k - 80) / 20.0
            elif k < 20:
                sub = (k - 20) / 20.0
            sub = max(-1.0, min(1.0, sub))
            scores.append((sub, weights.get("stochastic", 0.8)))

        obv = df["obv"].dropna()
        if len(obv) >= cfg["tolerance"].get("obv_slope_lookback", 5):
            lb = cfg["tolerance"].get("obv_slope_lookback", 5)
            slope_val = obv.iloc[-1] - obv.iloc[-lb]
            pct_change_std = obv.pct_change().std()
            denom = pct_change_std if pct_change_std not in (0, None) else 1.0
            sub = float(np.tanh((slope_val / (denom if denom != 0 else 1.0)) * 0.5))
            scores.append((sub, weights.get("obv", 1.0)))

        if not scores:
            print("[TV_DEBUG] no indicator scores computed -> Neutral")
            return 0.0, "Neutral"

        num = sum(s * w for (s, w) in scores)
        denom = sum(abs(w) for (_, w) in scores) or 1.0
        score = float(num / denom)

        adx = last.get("adx")
        if adx is not None and not pd.isna(adx):
            adx_cfg = cfg.get("adx", {})
            thr = adx_cfg.get("threshold", 25)
            mult = adx_cfg.get("multiplier", 1.25)
            if adx >= thr:
                score *= mult

        score = max(-2.0, min(2.0, score))

        t = cfg.get("thresholds", {"strong_buy": 0.6, "buy": 0.25, "sell": -0.25, "strong_sell": -0.6})
        label = "Neutral"
        if score >= t["strong_buy"]:
            label = "Strong Buy"
        elif score >= t["buy"]:
            label = "Buy"
        elif score <= t["strong_sell"]:
            label = "Strong Sell"
        elif score <= t["sell"]:
            label = "Sell"

        try:
            print(f"[TV_DEBUG] tf={tf} score={score:.4f} label={label}")
        except Exception:
            print(f"[TV_DEBUG] tf={tf} score={score} label={label}")
        return score, label
    except Exception as e:
        print(f"[TV_DEBUG] compute_tv_rating error: {e}")
        return 0.0, "Neutral"


def compute_mtf_alignment(get_closes_fn: Callable[[str], List[float]], price: float, mtf_tfs: List[str], mtf_slope_lookback: int = 3) -> Dict[str, AnyT]:
    tf_states: Dict[str, Dict[str, AnyT]] = {}
    negative_tfs: List[str] = []
    one_d_hist: List[float] = []

    for tf in mtf_tfs:
        closes = get_closes_fn(tf) or []
        _, _, hist = compute_macd_from_closes(closes, include_price=price)
        hist = hist or []
        cur = hist[-1] if hist else None
        prev = hist[-2] if len(hist) >= 2 else None
        is_positive = cur is not None and cur > 0
        is_flip = (prev is not None and prev < 0 and cur is not None and cur > 0)
        tf_states[tf] = {"cur": cur, "prev": prev, "is_positive": is_positive, "is_flip": is_flip, "slope": None}
        if tf == "D":
            one_d_hist = hist
        if not is_positive:
            negative_tfs.append(tf)

    if not negative_tfs:
        return {"status": "aligned", "tfs": tf_states, "negative_tfs": [], "one_d_slope": None}

    if negative_tfs == ["D"]:
        if slope is not None:
            one_d_slope = slope(one_d_hist, lookback=mtf_slope_lookback) if one_d_hist else None
            if one_d_slope is not None and one_d_slope > 0:
                tf_states["D"]["slope"] = one_d_slope
                return {"status": "daily_rising", "tfs": tf_states, "negative_tfs": ["D"], "one_d_slope": one_d_slope}
    return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}
