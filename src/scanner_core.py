"""
scanner_core.py

Pure, synchronous helpers and deterministic computations extracted from scanner.py.

These functions:
- Do NOT perform network I/O
- Accept inputs (klines, closes, volume dicts, config) to be unit-testable
- Preserve behavior of original helpers (normalization, MACD wrapper, TV rating, quantize, MTF alignment)

Note: This module will attempt to import pandas_ta (preferred) or the alternative 'ta' package and will attempt to compute indicators using whichever is available. If no supported TA API is present, compute_tv_rating will return neutral.
"""
import math
import json
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
    - If max_age_sec <= 0: check is disabled -> return True (accept flips of any age).
    - If start_at is None/unparseable: return True (conservative: allow).
    - start_at may be seconds or milliseconds; detect and normalize.
    - Return True if (now - start_sec) <= max_age_sec, else False.
    """
    try:
        # If max_age_sec <= 0, treat as disabled (accept any age)
        if max_age_sec is None or int(max_age_sec) <= 0:
            return True

        if start_at is None:
            return True

        # convert to float seconds (handle milliseconds)
        start_sec = float(start_at) / 1000.0 if int(start_at) > 10000000000 else float(start_at)
        age = now - start_sec
        return age <= float(max_age_sec)
    except Exception:
        # On any error be permissive (do not block signals)
        return True


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
            # Best-effort: skip malformed item
            continue

    return out


def quantize_qty(qty: float, step: Optional[float], min_qty: Optional[float]) -> float:
    """
    Quantize a raw quantity to the nearest valid step size and respect min_qty.
    """
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


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None):
    """
    Compute MACD histogram from a list of closes (floats).
    include_price: when provided, overwrites the last close value with current price.
    Returns: (macd_line, signal_line, hist) â€” each as list-like (macd_histogram implementation dependent)
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
        current_price = float(include_price)
        if data:
            data[-1] = current_price
        else:
            data.append(current_price)

    macd_line, signal_line, hist = macd_histogram(data)
    try:
        hist = [None if v is None else float(v) for v in (hist or [])]
    except Exception:
        pass
    return macd_line, signal_line, hist


def detect_flip_current_open(hist: List[float], hist_threshold: float = 0.0) -> bool:
    """
    Detect zero-cross flip from negative (or <=0) to positive on last candle.
    hist is a list where last item is most recent.
    """
    if not hist or len(hist) < 2:
        return False
    prev = hist[-2]
    cur = hist[-1]
    if prev is None or cur is None:
        return False
    try:
        zero_cross = prev <= 0 and cur > 0
        return zero_cross
    except Exception:
        return False


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

        # Scoring
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
        one_d_slope = slope(one_d_hist, lookback=mtf_slope_lookback) if one_d_hist else None
        if one_d_slope is not None and one_d_slope > 0:
            tf_states["D"]["slope"] = one_d_slope
            return {"status": "daily_rising", "tfs": tf_states, "negative_tfs": ["D"], "one_d_slope": one_d_slope}

    return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}
