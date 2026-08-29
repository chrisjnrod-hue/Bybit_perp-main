"""
scanner_core.py

Pure, synchronous helpers and deterministic computations used by the scanner.
These routines do NOT perform network I/O so they can be unit-tested.
"""
from __future__ import annotations

import math
import statistics
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Callable, Dict, List, Optional, Tuple

getcontext().prec = 28

# Optional numerical libs
try:
    import pandas as pd  # type: ignore
    import numpy as np  # type: ignore
except Exception:
    pd = None  # type: ignore
    np = None  # type: ignore

# Optional TA modules (pandas_ta or ta)
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
        _ta_module = None
        _PANDAS_TA_AVAILABLE = False
        _PANDAS_TA_STYLE = False

# Try to import optional helpers (macd, slope). If missing, fallback implementations are provided below.
try:
    from .macd import macd_histogram, slope  # type: ignore
except Exception:
    macd_histogram = None
    slope = None  # type: ignore

# Exported symbols
__all__ = [
    "tf_to_seconds",
    "is_candle_age_acceptable",
    "normalize_klines",
    "quantize_qty",
    "compute_macd_from_closes",
    "detect_flip_current_open",
    "compute_24h_volume_change_from",
    "compute_tv_rating_from",
    "compute_mtf_alignment",
]

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
            if s == "D":
                return 24 * 3600
            try:
                return int(s[:-1]) * 86400
            except Exception:
                return 24 * 3600
        return int(s) * 60
    except Exception:
        return 60


def is_candle_age_acceptable(start_at: Optional[int], now: float, max_age_sec: Optional[int]) -> bool:
    """
    Determine if a candle start_at is fresh enough compared to now.
    - If max_age_sec is None or <=0: disabled -> return True.
    - If start_at is None: cannot check -> return True.
    Accept both seconds and milliseconds timestamps.
    """
    try:
        if max_age_sec is None or int(max_age_sec) <= 0:
            return True
        if start_at is None:
            return True
        # detect ms vs s
        try:
            si = int(start_at)
        except Exception:
            return True
        start_sec = float(si) / 1000.0 if si > 1_000_000_0000 else float(si)
        return (now - start_sec) <= float(max_age_sec)
    except Exception:
        return True


# --------------------
# Normalization helpers
# --------------------
def normalize_klines(raw_klines: Any, tf: str) -> List[Dict[str, Any]]:
    """
    Normalize many API kline shapes into a list of dicts:
    { "start_at", "open", "high", "low", "close", "volume", "is_closed"(optional) }
    """
    out: List[Dict[str, Any]] = []
    if not raw_klines:
        return out

    # unwrap common envelope shapes
    if isinstance(raw_klines, dict):
        for key in ("list", "result", "data"):
            if key in raw_klines and isinstance(raw_klines[key], (list, dict)):
                raw_klines = raw_klines[key]
                break

    if not isinstance(raw_klines, (list, tuple)):
        raw_seq = [raw_klines] if raw_klines else []
    else:
        raw_seq = raw_klines

    for item in raw_seq:
        try:
            if isinstance(item, (list, tuple)):
                # typical [start, open, high, low, close, volume, ...]
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
                    out.append({"start_at": start, "open": open_p, "high": high, "low": low, "close": close, "volume": vol})
                continue

            if isinstance(item, dict):
                start = item.get("start_at") or item.get("open_time") or item.get("t") or item.get("timestamp") or item.get("start") or item.get("time")
                open_p = item.get("open") or item.get("openPrice") or item.get("o")
                high = item.get("high") or item.get("h")
                low = item.get("low") or item.get("l")
                close = item.get("close") or item.get("close_price") or item.get("c") or item.get("last_price") or item.get("Close")
                vol = item.get("volume") or item.get("vol") or item.get("turnover") or item.get("v") or item.get("quoteAsset")
                is_closed = item.get("isClosed")
                if is_closed is None:
                    is_closed = item.get("is_closed") or item.get("complete") or item.get("confirmed")
                try:
                    if start is not None:
                        start = int(start)
                except Exception:
                    start = None
                for name, value in (("open", open_p), ("high", high), ("low", low), ("close", close), ("volume", vol)):
                    try:
                        if value is not None and name != "volume":
                            locals()[name] = float(value)
                        elif value is not None and name == "volume":
                            vol = float(value)
                    except Exception:
                        if name == "open":
                            open_p = None
                        elif name == "high":
                            high = None
                        elif name == "low":
                            low = None
                        elif name == "close":
                            close = None
                        elif name == "volume":
                            vol = None
                if close is not None:
                    out.append({"start_at": start, "open": open_p, "high": high, "low": low, "close": close, "volume": vol, "is_closed": is_closed})
                continue
        except Exception:
            # skip malformed
            continue

    return out


# --------------------
# Quantize helper
# --------------------
def quantize_qty(qty: Optional[float], step: Optional[float], min_qty: Optional[float]) -> float:
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
# MACD helpers (pure-python fallback)
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
    """Simple EMA-based MACD fallback calculation. Returns (macd_series, signal_series, hist_series)."""
    if not closes:
        return [], [], []

    def ema(values: List[float], period: int) -> List[float]:
        if not values or period < 1:
            return []
        out: List[float] = []
        alpha = 2.0 / (period + 1.0)
        # Initialize with simple SMA for first point if possible
        if len(values) < period:
            s = sum(values) / len(values)
            out = [s for _ in values]
            return out
        sma = sum(values[:period]) / period
        out.append(sma)
        prev = sma
        for v in values[period:]:
            prev = (float(v) * alpha) + (prev * (1.0 - alpha))
            out.append(prev)
        return out

    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    if not fast_ema or not slow_ema:
        return [], [], []
    # align lengths
    length = min(len(fast_ema), len(slow_ema))
    macd_series = [fast_ema[i] - slow_ema[i] for i in range(length)]
    signal_series = ema(macd_series, signal) if macd_series else []
    if not signal_series:
        return [], [], []
    hist_len = min(len(macd_series), len(signal_series))
    hist_series = [macd_series[i] - signal_series[i] for i in range(hist_len)]
    return macd_series[-hist_len:], signal_series[-hist_len:], hist_series


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Returns (macd_last, signal_last, hist_list) where hist_list is oldest->newest plain list.
    Uses optional external macd_histogram helper if available, otherwise uses the fallback.
    """
    # validate closes
    data: List[float] = []
    for c in closes:
        try:
            if c is None:
                continue
            val = float(c)
            if math.isfinite(val):
                data.append(val)
        except Exception:
            continue

    if include_price is not None and data:
        try:
            p = float(include_price)
            if math.isfinite(p):
                data[-1] = p
        except Exception:
            pass
    elif include_price is not None and not data:
        try:
            p = float(include_price)
            if math.isfinite(p):
                data.append(p)
        except Exception:
            pass

    # Try external helper if present
    if macd_histogram is not None:
        try:
            macd_line_raw, signal_line_raw, hist_raw = macd_histogram(data)
            hist_list = _clean_hist(hist_raw)
            macd_last = None
            signal_last = None
            try:
                if hasattr(macd_line_raw, "__len__") and len(macd_line_raw):
                    macd_last = float(macd_line_raw[-1])
            except Exception:
                macd_last = None
            try:
                if hasattr(signal_line_raw, "__len__") and len(signal_line_raw):
                    signal_last = float(signal_line_raw[-1])
            except Exception:
                signal_last = None
            return macd_last, signal_last, hist_list
        except Exception:
            # fallback silently
            pass

    macd_series, signal_series, hist_series = _fallback_macd(data, fast=fast, slow=slow, signal=signal)
    if not hist_series:
        return None, None, []
    macd_last = float(macd_series[-1]) if macd_series else None
    signal_last = float(signal_series[-1]) if signal_series else None
    return macd_last, signal_last, hist_series


def _safe_last(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if hasattr(v, "iloc"):
            if len(v) == 0:
                return None
            return float(v.iloc[-1])
        if hasattr(v, "tolist"):
            lst = v.tolist()
            if not lst:
                return None
            return float(lst[-1])
        if isinstance(v, (list, tuple)):
            if not v:
                return None
            return float(v[-1])
        return float(v) if _is_finite_number(v) else None
    except Exception:
        return None


# --------------------
# Flip detection
# --------------------
def detect_flip_current_open(hist: List[float], hist_threshold: Optional[float] = None, std_mult: float = 0.05, abs_min: float = 1e-9, lookback: int = 1) -> bool:
    """
    Return True when histogram flips from negative (prev) to positive (current) with a noise threshold.
    """
    try:
        clean = _clean_hist(hist)
        if not clean or len(clean) < (lookback + 1):
            return False
        prev = clean[-(lookback + 1)]
        cur = clean[-1]
        if prev is None or cur is None:
            return False
        if hist_threshold is None:
            try:
                hist_std = statistics.pstdev(clean) if len(clean) >= 2 else 0.0
            except Exception:
                hist_std = 0.0
            hist_threshold = max(abs_min, abs(hist_std) * std_mult)
            hist_threshold = min(hist_threshold, 0.01)
        return (prev < -1e-9) and (cur > hist_threshold)
    except Exception:
        return False


# --------------------
# Volume helper
# --------------------
def compute_24h_volume_change_from(vol_data: Optional[Dict[str, float]]) -> Optional[float]:
    try:
        if not vol_data:
            return None
        prev_vol = vol_data.get("previous", 0)
        curr_vol = vol_data.get("current", 0)
        if prev_vol <= 0:
            return None
        change = (curr_vol - prev_vol) / prev_vol
        return min(change, 1.0)
    except Exception:
        return None


# --------------------
# TV rating & MTF alignment
# --------------------
def compute_tv_rating_from(klines: List[Dict[str, Any]], cfg: Dict[str, Any], tf: Optional[str] = None, price: Optional[float] = None) -> Tuple[float, str]:
    """
    Compute a numeric TV-style technical rating. If pandas or indicators are missing,
    return Neutral (0.0, "Neutral") to avoid failing imports.
    cfg is expected to contain indicators, weights, tolerance, thresholds keys used below.
    """
    try:
        if not cfg.get("enabled", True):
            return 0.0, "Neutral"

        if not _PANDAS_TA_AVAILABLE or pd is None or np is None:
            return 0.0, "Neutral"

        # require enough candles
        ma_pairs = cfg.get("indicators", {}).get("ma_pairs", [])
        ma_max = max([n for pair in ma_pairs for n in pair]) if ma_pairs else 50
        min_candles = max(26, ma_max + 5)
        if not klines or len(klines) < min_candles:
            return 0.0, "Neutral"

        df = pd.DataFrame(klines)
        for col in ("open", "high", "low", "close", "volume"):
            if col not in df.columns:
                df[col] = np.nan
        df = df.dropna(subset=["close"]).copy()

        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

        if price is not None:
            try:
                df.at[df.index[-1], "close"] = float(price)
            except Exception:
                pass

        # moving averages
        ma_lengths = sorted(set([n for pair in ma_pairs for n in pair])) if ma_pairs else []
        for l in ma_lengths:
            sma_series = None
            try:
                if _PANDAS_TA_STYLE:
                    sma_series = _ta_module.sma(df["close"], length=int(l))
                else:
                    sma_series = _ta_module.trend.SMAIndicator(df["close"], window=int(l)).sma_indicator()
            except Exception:
                sma_series = None
            if sma_series is not None:
                df[f"sma_{l}"] = sma_series

        # MACD
        macd_cfg = cfg.get("indicators", {}).get("macd", [12, 26, 9])
        macd_res = None
        try:
            if _PANDAS_TA_STYLE:
                macd_res = _ta_module.macd(df["close"], fast=int(macd_cfg[0]), slow=int(macd_cfg[1]), signal=int(macd_cfg[2]))
            else:
                macd_obj = _ta_module.trend.MACD(df["close"], window_slow=int(macd_cfg[1]), window_fast=int(macd_cfg[0]), window_sign=int(macd_cfg[2]))
                import pandas as _pd  # local
                macd_res = _pd.DataFrame({
                    "MACD": macd_obj.macd(),
                    "MACD_signal": macd_obj.macd_signal(),
                    "MACD_hist": macd_obj.macd_diff()
                }, index=df.index)
        except Exception:
            macd_res = None
        if macd_res is not None and hasattr(macd_res, "columns"):
            hist_col = next((c for c in macd_res.columns if "H" in str(c).upper() or "diff" in str(c).lower()), None)
            if hist_col:
                df["macd_hist"] = macd_res[hist_col]
            else:
                df["macd_hist"] = macd_res.iloc[:, -1] if len(macd_res.columns) > 0 else 0.0
        else:
            df["macd_hist"] = 0.0

        # RSI
        rsi_period = int(cfg.get("indicators", {}).get("rsi_period", 14))
        rsi_series = None
        try:
            if _PANDAS_TA_STYLE:
                rsi_series = _ta_module.rsi(df["close"], length=rsi_period)
            else:
                rsi_series = _ta_module.momentum.RSIIndicator(df["close"], window=rsi_period).rsi()
        except Exception:
            rsi_series = None
        df["rsi"] = rsi_series

        # other indicators are optional: stoch, adx, obv, bbands
        try:
            stoch_cfg = cfg.get("indicators", {}).get("stochastic", [14, 3, 3])
            st = None
            try:
                if _PANDAS_TA_STYLE:
                    st = _ta_module.stoch(high=df["high"], low=df["low"], close=df["close"], k=int(stoch_cfg[0]), d=int(stoch_cfg[1]))
                else:
                    st = _ta_module.momentum.StochasticOscillator(high=df["high"], low=df["low"], close=df["close"], window=int(stoch_cfg[0]), smooth_window=int(stoch_cfg[1]))
            except Exception:
                st = None
            if st is not None and hasattr(st, "columns"):
                st_col = next((c for c in st.columns if "STOCH" in str(c).upper() or "k" in str(c).lower()), None)
                if st_col:
                    df["stoch_k"] = st[st_col]
        except Exception:
            pass

        try:
            obv = None
            if _PANDAS_TA_STYLE:
                obv = _ta_module.obv(df["close"], df["volume"])
            else:
                obv = _ta_module.volume.OnBalanceVolumeIndicator(close=df["close"], volume=df["volume"]).on_balance_volume()
            df["obv"] = obv
        except Exception:
            df["obv"] = None

        # score calculation (match original logic)
        last = df.iloc[-1]
        scores: List[Tuple[float, float]] = []
        weights = cfg.get("weights", {})

        for pair in cfg.get("indicators", {}).get("ma_pairs", []):
            short, long = pair
            s = last.get(f"sma_{short}")
            l = last.get(f"sma_{long}")
            if pd.isna(s) or pd.isna(l) or l == 0:
                continue
            pct = (s - l) / l
            tol = cfg.get("tolerance", {}).get("ma_pair_pct", 0.002)
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

        obv_series = df["obv"].dropna() if "obv" in df.columns and df["obv"] is not None else []
        if len(obv_series) >= cfg.get("tolerance", {}).get("obv_slope_lookback", 5):
            lb = cfg.get("tolerance", {}).get("obv_slope_lookback", 5)
            slope_val = obv_series.iloc[-1] - obv_series.iloc[-lb]
            pct_change_std = obv_series.pct_change().std()
            denom = pct_change_std if pct_change_std not in (0, None) else 1.0
            sub = float(np.tanh((slope_val / (denom if denom != 0 else 1.0)) * 0.5))
            scores.append((sub, weights.get("obv", 1.0)))

        if not scores:
            return 0.0, "Neutral"

        num = sum(s * w for (s, w) in scores)
        denom = sum(abs(w) for (_, w) in scores) or 1.0
        score = float(num / denom)

        adx = last.get("adx") if "adx" in last else None
        if adx is not None and not pd.isna(adx):
            adx_cfg = cfg.get("adx", {})
            thr = adx_cfg.get("threshold", 25)
            mult = adx_cfg.get("multiplier", 1.25)
            if adx >= thr:
                score *= mult

        score = max(-2.0, min(2.0, score))

        thresholds = cfg.get("thresholds", {"strong_buy": 0.6, "buy": 0.25, "sell": -0.25, "strong_sell": -0.6})
        label = "Neutral"
        if score >= thresholds["strong_buy"]:
            label = "Strong Buy"
        elif score >= thresholds["buy"]:
            label = "Buy"
        elif score <= thresholds["strong_sell"]:
            label = "Strong Sell"
        elif score <= thresholds["sell"]:
            label = "Sell"

        return score, label
    except Exception:
        return 0.0, "Neutral"


def compute_mtf_alignment(get_closes_fn: Callable[[str], List[float]], price: float, mtf_tfs: List[str], mtf_slope_lookback: int = 3) -> Dict[str, Any]:
    tf_states: Dict[str, Dict[str, Any]] = {}
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

    if negative_tfs == ["D"] and slope is not None:
        one_d_slope = slope(one_d_hist, lookback=mtf_slope_lookback) if one_d_hist else None
        if one_d_slope is not None and one_d_slope > 0:
            tf_states["D"]["slope"] = one_d_slope
            return {"status": "daily_rising", "tfs": tf_states, "negative_tfs": ["D"], "one_d_slope": one_d_slope}

    return {"status": "monitoring", "tfs": tf_states, "negative_tfs": negative_tfs, "one_d_slope": None}
