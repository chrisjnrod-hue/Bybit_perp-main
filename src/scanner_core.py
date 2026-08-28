# scanner_core.py
"""
Pure, synchronous helpers and deterministic computations extracted from scanner.py.

These functions:
- Do NOT perform network I/O
- Accept inputs (klines, closes, volume dicts, config) to be unit-testable
- Preserve behavior of original helpers (normalization, MACD wrapper, TV rating, quantize, MTF alignment)

Note: This module will attempt to import pandas_ta (preferred) or the alternative 'ta' package and will attempt to compute indicators using whichever is available. If no supported TA API is present, compute_tv_rating will return neutral.
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
    # Try the alternative 'ta' package (bukosabino) which uses class-based API
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
from .macd import macd_histogram, slope  # type: ignore


# --------------------
# Utilities / Helpers
# --------------------
def _is_finite_number(x: Any) -> bool:
    try:
        v = float(x)
        return math.isfinite(v)
    except Exception:
        return False


def _clean_hist(hist: Any) -> List[float]:
    """
    Convert many possible histogram shapes (list, tuple, numpy array, pandas Series)
    into a plain list of finite floats (oldest -> newest). Removes None/NaN/infinite.
    """
    out: List[float] = []
    if hist is None:
        return out
    try:
        # If it's a pandas Series or numpy array, turn into list
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


def _safe_last(v: Any) -> Optional[float]:
    """
    Return last numeric scalar from various shapes, else None.
    If v is list-like, returns last element converted to float, else if it's scalar numeric returns float.
    """
    if v is None:
        return None
    # pandas Series/DataFrame
    try:
        if hasattr(v, "iloc"):
            # Series-like
            if len(v) == 0:
                return None
            try:
                val = v.iloc[-1]
                return float(val) if _is_finite_number(val) else None
            except Exception:
                return None
        if hasattr(v, "tolist"):
            lst = v.tolist()
            if not lst:
                return None
            try:
                return float(lst[-1])
            except Exception:
                return None
    except Exception:
        pass
    # list/tuple
    try:
        if isinstance(v, (list, tuple)):
            if not v:
                return None
            return float(v[-1]) if _is_finite_number(v[-1]) else None
    except Exception:
        pass
    # scalar
    try:
        return float(v) if _is_finite_number(v) else None
    except Exception:
        return None


# --------------------
# MACD helpers
# --------------------
def _fallback_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    """
    Pure-Python fallback MACD calculation using EMA.
    Returns (macd_series, signal_series, hist_series) — each oldest->newest lists.
    EMA implementation uses standard smoothing alpha = 2 / (period + 1).
    """
    if not closes:
        return [], [], []

    def ema_series(values: List[float], period: int) -> List[float]:
        out: List[float] = []
        alpha = 2.0 / (period + 1.0)
        prev = None
        for v in values:
            if prev is None:
                prev = float(v)
            else:
                prev = (float(v) * alpha) + (prev * (1.0 - alpha))
            out.append(float(prev))
        return out

    # compute EMAs
    try:
        fast_ema = ema_series(closes, fast)
        slow_ema = ema_series(closes, slow)
        # macd series (fast - slow), align lengths (they are same length because we computed over same closes)
        macd_series = [f - s for f, s in zip(fast_ema, slow_ema)]
        # signal is EMA of macd_series
        signal_series = ema_series(macd_series, signal)
        hist_series = [m - s for m, s in zip(macd_series, signal_series)]
        return macd_series, signal_series, hist_series
    except Exception:
        return [], [], []


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Compute MACD histogram from a list of closes (floats).
    include_price: when provided, overwrites the last close value with current price.
    Returns: (macd_line_last, signal_line_last, hist_list) where:
      - macd_line_last, signal_line_last are last numeric values (or None)
      - hist_list is a list of floats (oldest -> newest); may be empty.
    This function is robust to pandas/numpy/ta outputs and provides a pure-python fallback.
    """
    data: List[float] = []
    for c in closes:
        try:
            if c is None:
                continue
            data.append(float(c))
        except Exception:
            continue

    if include_price is not None:
        try:
            current_price = float(include_price)
            if data:
                data[-1] = current_price
            else:
                data.append(current_price)
        except Exception:
            pass

    # Try existing macd_histogram helper first (keeps behaviour)
    try:
        macd_line_raw, signal_line_raw, hist_raw = macd_histogram(data)
        # hist may be list-like, numpy array, pandas Series — convert to list of floats
        hist_list = _clean_hist(hist_raw)
        macd_last = _safe_last(macd_line_raw)
        signal_last = _safe_last(signal_line_raw)
        return macd_last, signal_last, hist_list
    except Exception:
        # Fallback: compute MACD in pure python
        try:
            macd_series, signal_series, hist_series = _fallback_macd(data, fast=fast, slow=slow, signal=signal)
            macd_last = float(macd_series[-1]) if macd_series else None
            signal_last = float(signal_series[-1]) if signal_series else None
            return macd_last, signal_last, hist_series
        except Exception:
            return None, None, []


# --------------------
# Flip detection
# --------------------
def detect_flip_current_open(hist: List[float], hist_threshold: Optional[float] = None, std_mult: float = 0.05, abs_min: float = 1e-9, lookback: int = 1) -> bool:
    """
    Detect zero-cross flip from negative (or <=0) to positive on last candle, with noise gating.

    Parameters:
      hist: list-like of histogram values (oldest -> newest). Can contain None/NaN; will be cleaned.
      hist_threshold: optional explicit numeric threshold; if None a dynamic threshold is computed.
      std_mult: multiplier applied to sample std for dynamic threshold.
      abs_min: minimum absolute threshold to avoid triggering on tiny noise.
      lookback: how many candles back to consider as 'previous' (1 => prev = -2, cur = -1)

    Behavior:
      - Clean input to finite floats.
      - Need at least 2 clean values otherwise return False.
      - Compute prev = clean[-(lookback+1)], cur = clean[-1].
      - If hist_threshold is None compute threshold = max(abs_min, hist_std * std_mult).
      - Return True only if prev <= 0 and cur > threshold.
    """
    try:
        clean = _clean_hist(hist)
        if not clean or len(clean) < (lookback + 1):
            return False
        prev = clean[-(lookback + 1)]
        cur = clean[-1]
        # If either None/unfinites present after cleaning, bail
        if prev is None or cur is None:
            return False
        # Dynamic threshold if not provided
        if hist_threshold is None:
            try:
                hist_std = statistics.pstdev(clean) if len(clean) >= 2 else 0.0
            except Exception:
                hist_std = 0.0
            hist_threshold = max(abs_min, abs(hist_std) * std_mult)
        # Zero-cross with gating
        return (prev <= 0.0) and (cur > float(hist_threshold))
    except Exception:
        return False


# --------------------
# Volume helper (unchanged)
# --------------------
def compute_24h_volume_change_from(vol_data: Optional[Dict[str, float]]) -> Optional[float]:
    """
    Compute percentage change given a symbol's volume tracking dict:
      {"current": float, "previous": float}
    Returns None if insufficient data or prev <= 0. Clamps to 1.0 max.
    Also prints debug info to console for troubleshooting.
    """
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
        # Console debug
        try:
            print(f"[VOL_DEBUG] prev={prev_vol:.0f}, curr={curr_vol:.0f}, change={change:.4f} => result_clamped={result:.4f}")
        except Exception:
            print(f"[VOL_DEBUG] prev={prev_vol}, curr={curr_vol}, change={change}")
        return result
    except Exception as e:
        print(f"[VOL_DEBUG] compute_24h_volume_change error: {e}")
        return None


# --- Helper wrappers for indicator functions (work with pandas_ta or bukosabino/ta) ---
def _safe_sma(df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.sma(df_close, length=length)
        else:
            # bukosabino style
            return _ta_module.trend.SMAIndicator(df_close, window=int(length)).sma_indicator()
    except Exception:
        return None


def _safe_macd(df_close, fast: int, slow: int, signal: int):
    """
    Return a DataFrame-like object (or None). Try pandas_ta.macd first, else construct DataFrame for macd, macd_signal, macd_diff.
    """
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.macd(df_close, fast=fast, slow=slow, signal=signal)
        else:
            # bukosabino style: MACD class
            macd_obj = _ta_module.trend.MACD(df_close, window_slow=int(slow), window_fast=int(fast), window_sign=int(signal))
            # Build dataframe-like structure (pandas Series/Frame) if pandas available
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
            # bukosabino StochasticOscillator: stoch()
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
# TV rating and MTF alignment (unchanged core logic, but robust inputs)
# --------------------
def compute_tv_rating_from(klines: List[Dict[str, AnyT]], cfg: Dict[str, AnyT], tf: Optional[str] = None, price: Optional[float] = None) -> Tuple[float, str]:
    """
    Compute a TradingView-like normalized score using pandas/ta or fallback 'ta' and the TECHNICAL_RATING-style cfg.
    Inputs:
      klines: normalized kline dicts (with 'close','open','high','low','volume')
      cfg: TECHNICAL_RATING config dict (must contain 'indicators' etc.)
      tf: optional timeframe string (not used by computation but kept for compatibility)
      price: optional current price to apply to last close
    Returns: (score, label)

    This function prints concise debug messages to console describing why computation
    may have returned a neutral score (missing libs, insufficient candles, conversion errors),
    and prints the final computed score when successful.
    """
    # Debug start
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

        # Indicators computation with debug markers using safe wrappers
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
                # try to find a histogram-like column
                macd_hist_col = next((c for c in macd_result.columns if "MACD" in str(c).upper() and ("H" in str(c).upper() or "diff" in str(c).lower())), None)
                if macd_hist_col:
                    df["macd_hist"] = macd_result[macd_hist_col]
                else:
                    # fall back to last column
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

        # Scoring (unchanged)
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

        # Final debug output
        try:
            print(f"[TV_DEBUG] tf={tf} score={score:.4f} label={label}")
        except Exception:
            print(f"[TV_DEBUG] tf={tf} score={score} label={label}")
        return score, label
    except Exception as e:
        print(f"[TV_DEBUG] compute_tv_rating error: {e}")
        return 0.0, "Neutral"


def compute_mtf_alignment(get_closes_fn: Callable[[str], List[float]], price: float, mtf_tfs: List[str], mtf_slope_lookback: int = 3) -> Dict[str, AnyT]:
    """
    Evaluate MTF alignment across timeframes.
    get_closes_fn(tf) -> list of closes for that tf (most recent last).
    price: include_price applied to MACD calculations.
    mtf_tfs: list of TFs to evaluate (e.g., ["5","15","60","240","D"])
    mtf_slope_lookback: lookback for daily slope
    """
    tf_states: Dict[str, Dict[str, AnyT]] = {}
    negative_tfs: List[str] = []
    one_d_hist: List[float] = []

    for tf in mtf_tfs:
        closes = get_closes_fn(tf) or []
        _, _, hist = compute_macd_from_closes(closes, include_price=price)
        hist = hist or []
        cur = hist[-1] if hist else None
        prev = hist[-2] if len(hist) >= 2 else None

        # Use robust flip detection with small threshold (same defaults as detect_flip_current_open)
        is_positive = cur is not None and cur > 0
        is_flip = detect_flip_current_open(hist, None, std_mult=0.05, abs_min=1e-9, lookback=1)

        tf_states[tf] = {"cur": cur, "prev": prev, "is_positive": is_positive, "is_flip": is_flip, "slope": None}
        if tf == "D":
            one_d_hist = hist
        if not is_positive:
            negative_tfs.append(tf)

    if not negative_tfs:
        return {"status": "aligned", "tfs": tf_states, "negative_tfs": [], "one_d_slope": None}

    if negative_tfs == ["D"]:
        one_d_slope = slope(one_d_hist, lookback=mtf_slope_lookback) if one_d_hist else None
        if one_d_slope is not None and one_d_slope > 0:
            tf_states["D"]["slope"] = one_d_slope
            return {"status": "daily_rising", "tfs": tf_states, "negative_tfs": ["D"], "one_d_slope": one_d_slope}

    return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}
