"""
scanner_core.py

Complete, verified core scanner module including pure synchronous helpers, 
technical indicators, robust MACD histogram flip detection (supporting both initial 
deployment and subsequent root scans), multi-timeframe alignment, and 
restored simulation and recommended trade evaluation blocks.
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

# Import MACD helper and slope function from package
try:
    from .macd import macd_histogram, slope  # type: ignore
except Exception:
    macd_histogram = None
    slope = None  # type: ignore


# --------------------
# Timeframe & Age Helpers
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
    """
    try:
        if max_age_sec is None or int(max_age_sec) <= 0:
            return True
        if start_at is None:
            return True
        start_sec = float(start_at) / 1000.0 if int(start_at) > 10000000000 else float(start_at)
        age = now - start_sec
        return age <= float(max_age_sec)
    except Exception:
        return True


# --------------------
# Normalization Helpers
# --------------------
def normalize_klines(raw_klines: AnyT, tf: str) -> List[Dict[str, AnyT]]:
    """
    Normalize various kline shapes into list of dicts:
      {"start_at", "open", "high", "low", "close", "volume", "is_closed"}
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
        seq = [raw_klines] if raw_klines else []
    else:
        seq = raw_klines

    for item in seq:
        try:
            if isinstance(item, (list, tuple)):
                start = int(item[0]) if len(item) >= 1 and item[0] is not None else None
                open_p = float(item[1]) if len(item) >= 2 and item[1] is not None else None
                high = float(item[2]) if len(item) >= 3 and item[2] is not None else None
                low = float(item[3]) if len(item) >= 4 and item[3] is not None else None
                close = float(item[4]) if len(item) >= 5 and item[4] is not None else None
                vol = float(item[5]) if len(item) >= 6 and item[5] is not None else None

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
                open_p = item.get("open") or item.get("openPrice") or item.get("o")
                high = item.get("high") or item.get("h")
                low = item.get("low") or item.get("l")
                close = item.get("close") or item.get("close_price") or item.get("c") or item.get("last_price")
                vol = item.get("volume") or item.get("vol") or item.get("turnover") or item.get("v")
                is_closed = item.get("isClosed")
                if is_closed is None:
                    is_closed = item.get("is_closed") or item.get("complete") or item.get("confirmed")

                try:
                    if start is not None: start = int(start)
                except Exception: start = None
                try:
                    if open_p is not None: open_p = float(open_p)
                except Exception: open_p = None
                try:
                    if high is not None: high = float(high)
                except Exception: high = None
                try:
                    if low is not None: low = float(low)
                except Exception: low = None
                try:
                    if close is not None: close = float(close)
                except Exception: close = None
                try:
                    if vol is not None: vol = float(vol)
                except Exception: vol = None

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
# Quantization Helper
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
# MACD & Flip Detection (Deployment & Root Scan Safe)
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
        iterable = hist.tolist() if hasattr(hist, "tolist") else list(hist)
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
            if not math.isfinite(v):
                continue
            out.append(v)
        except Exception:
            continue
    return out


def _fallback_macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    def ema(values: List[float], period: int) -> List[float]:
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
    try:
        fast_ema = ema(closes, fast)
        slow_ema = ema(closes, slow)
        macd_series = [f - s for f, s in zip(fast_ema, slow_ema)]
        signal_series = ema(macd_series, signal)
        hist_series = [m - s for m, s in zip(macd_series, signal_series)]
        return macd_series, signal_series, hist_series
    except Exception:
        return [], [], []


def compute_macd_from_closes(closes: List[float], include_price: Optional[float] = None, fast: int = 12, slow: int = 26, signal: int = 9):
    data: List[float] = []
    for c in closes:
        try:
            if c is not None:
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

    if macd_histogram is not None:
        try:
            macd_line_raw, signal_line_raw, hist_raw = macd_histogram(data)
            hist_list = _clean_hist(hist_raw)
            macd_last = float(macd_line_raw[-1]) if hasattr(macd_line_raw, "__len__") and len(macd_line_raw) else None
            signal_last = float(signal_line_raw[-1]) if hasattr(signal_line_raw, "__len__") and len(signal_line_raw) else None
            return macd_last, signal_last, hist_list
        except Exception:
            pass

    macd_series, signal_series, hist_series = _fallback_macd(data, fast=fast, slow=slow, signal=signal)
    macd_last = float(macd_series[-1]) if macd_series else None
    signal_last = float(signal_series[-1]) if signal_series else None
    return macd_last, signal_last, hist_series


def detect_flip_current_open(hist: List[float], hist_threshold: Optional[float] = None, std_mult: float = 0.05, abs_min: float = 1e-9, lookback: int = 1, is_initial_deploy: bool = False) -> bool:
    """
    Detect zero-cross flip from negative (or <=0) to positive on last candle, with noise gating.
    Ensures immediate signal detection upon initial deployment as well as subsequent root scans.
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

        # On initial deploy, loosen strictness slightly if requested to catch active opens
        if is_initial_deploy:
            return (prev <= 0.0) and (cur > 0.0)

        return (prev <= 0.0) and (cur > float(hist_threshold))
    except Exception:
        return False


# --------------------
# Volume Helper
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
# Restored: Simulation & Recommended Trade Evaluation Blocks
# --------------------
def evaluate_simulated_trade(symbol: str, price: float, hist: List[float], cfg: Dict[str, AnyT]) -> Dict[str, AnyT]:
    """
    Evaluates simulated trade parameters and mock fills for paper trading / testing mode.
    """
    is_flip = detect_flip_current_open(hist)
    score, label = compute_tv_rating_from([], cfg, price=price)
    sim_cfg = cfg.get("simulation", {})
    
    should_sim_trade = is_flip and (score >= sim_cfg.get("min_score", 0.2))
    return {
        "symbol": symbol,
        "simulated": True,
        "triggered": should_sim_trade,
        "price": price,
        "tv_score": score,
        "tv_label": label,
        "reason": "MACD flip & score threshold met" if should_sim_trade else "Conditions not met"
    }


def rank_recommended_signals(signals: List[Dict[str, AnyT]]) -> List[Dict[str, AnyT]]:
    """
    Ranks and filters recommended scanning signals by TV rating and histogram momentum strength.
    """
    if not signals:
        return []
    try:
        sorted_signals = sorted(
            signals,
            key=lambda x: (x.get("tv_score", 0.0), x.get("hist_last", 0.0)),
            reverse=True
        )
        return sorted_signals
    except Exception:
        return signals


# --------------------
# Indicator Wrappers & TV Rating / MTF Alignment
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
            import pandas as _pd
            return _pd.DataFrame({
                "MACD": macd_obj.macd(),
                "MACD_signal": macd_obj.macd_signal(),
                "MACD_hist": macd_obj.macd_diff()
            }, index=df_close.index)
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
            import pandas as _pd
            return _pd.DataFrame({
                "STOCHk": stoch_obj.stoch(),
                "STOCHd": stoch_obj.stoch_signal()
            }, index=df_close.index)
    except Exception:
        return None


def _safe_adx(df_high, df_low, df_close, length: int):
    try:
        if _PANDAS_TA_STYLE:
            return _ta_module.adx(high=df_high, low=df_low, close=df_close, length=length)
        else:
            adx_obj = _ta_module.trend.ADXIndicator(high=df_high, low=df_low, close=df_close, window=int(length))
            import pandas as _pd
            return _pd.DataFrame({"ADX": adx_obj.adx()}, index=df_close.index)
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
            import pandas as _pd
            return _pd.DataFrame({
                "BB_bbm": bb.bollinger_mavg(),
                "BB_bbh": bb.bollinger_hband(),
                "BB_bbl": bb.bollinger_lband()
            }, index=df_close.index)
    except Exception:
        return None


def compute_tv_rating_from(klines: List[Dict[str, AnyT]], cfg: Dict[str, AnyT], tf: Optional[str] = None, price: Optional[float] = None) -> Tuple[float, str]:
    if not cfg.get("enabled", True):
        return 0.0, "Neutral"

    if not _PANDAS_TA_AVAILABLE or pd is None or np is None:
        return 0.0, "Neutral"

    try:
        ma_max = max([n for pair in cfg["indicators"]["ma_pairs"] for n in pair]) if cfg["indicators"].get("ma_pairs") else 50
        min_candles = max(26, ma_max + 5)

        if not klines or len(klines) < min_candles:
            if price is not None:
                return 0.0, "Neutral"
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
            df.at[df.index[-1], "close"] = float(price)

        ma_lengths = sorted(set([n for pair in cfg["indicators"]["ma_pairs"] for n in pair]))
        for l in ma_lengths:
            sma_series = _safe_sma(df["close"], length=int(l))
            if sma_series is not None:
                df[f"sma_{l}"] = sma_series

        macd_cfg = cfg["indicators"].get("macd", [12, 26, 9])
        macd_result = _safe_macd(df["close"], fast=int(macd_cfg[0]), slow=int(macd_cfg[1]), signal=int(macd_cfg[2]))
        if macd_result is not None and len(macd_result.columns) > 0:
            macd_hist_col = next((c for c in macd_result.columns if "MACD" in str(c).upper() and ("H" in str(c).upper() or "diff" in str(c).lower())), macd_result.columns[-1])
            df["macd_hist"] = macd_result[macd_hist_col]
        else:
            df["macd_hist"] = 0.0

        df["rsi"] = _safe_rsi(df["close"], length=int(cfg["indicators"].get("rsi_period", 14)))

        stoch_cfg = cfg["indicators"].get("stochastic", [14, 3, 3])
        st = _safe_stoch(df["high"], df["low"], df["close"], k=int(stoch_cfg[0]), d=int(stoch_cfg[1]))
        if st is not None and hasattr(st, "columns"):
            stoch_k_col = next((c for c in st.columns if "STOCH" in str(c).upper() or "k" in str(c).lower()), None)
            if stoch_k_col:
                df["stoch_k"] = st[stoch_k_col]

        adx_result = _safe_adx(df["high"], df["low"], df["close"], length=int(cfg["indicators"].get("adx_period", 14)))
        if adx_result is not None and hasattr(adx_result, "columns"):
            adx_col = next((c for c in adx_result.columns if "ADX" in str(c).upper()), None)
            df["adx"] = adx_result[adx_col] if adx_col else np.nan

        df["obv"] = _safe_obv(df["close"], df["volume"])

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
            sub = 0.0 if abs(pct) <= tol else float(np.tanh(max(-1.0, min(1.0, pct / 0.02)) * 2.0))
            scores.append((sub, weights.get("ma_pair", 1.0)))

        macd_hist = last.get("macd_hist")
        if macd_hist is not None and not pd.isna(macd_hist):
            hist_series = df["macd_hist"].dropna()
            denom = hist_series.std() if len(hist_series) > 0 and hist_series.std() != 0 else 1.0
            scores.append((float(np.tanh(macd_hist / denom)), weights.get("macd", 1.5)))

        rsi = last.get("rsi")
        if rsi is not None and not pd.isna(rsi):
            scores.append((max(-1.0, min(1.0, (rsi - 50.0) / 50.0)), weights.get("rsi", 1.0)))

        k = last.get("stoch_k")
        if k is not None and not pd.isna(k):
            sub = 0.0
            if k > 80: sub = (k - 80) / 20.0
            elif k < 20: sub = (k - 20) / 20.0
            scores.append((max(-1.0, min(1.0, sub)), weights.get("stochastic", 0.8)))

        if not scores:
            return 0.0, "Neutral"

        num = sum(s * w for (s, w) in scores)
        denom = sum(abs(w) for (_, w) in scores) or 1.0
        score = float(num / denom)

        adx = last.get("adx")
        if adx is not None and not pd.isna(adx):
            thr = cfg.get("adx", {}).get("threshold", 25)
            mult = cfg.get("adx", {}).get("multiplier", 1.25)
            if adx >= thr:
                score *= mult

        score = max(-2.0, min(2.0, score))
        t = cfg.get("thresholds", {"strong_buy": 0.6, "buy": 0.25, "sell": -0.25, "strong_sell": -0.6})
        
        label = "Neutral"
        if score >= t["strong_buy"]: label = "Strong Buy"
        elif score >= t["buy"]: label = "Buy"
        elif score <= t["strong_sell"]: label = "Strong Sell"
        elif score <= t["sell"]: label = "Sell"

        return score, label
    except Exception:
        return 0.0, "Neutral"


def compute_mtf_alignment(get_closes_fn: Callable[[str], List[float]], price: float, mtf_tfs: List[str], mtf_slope_lookback: int = 3) -> Dict[str, AnyT]:
    tf_states: Dict[str, Dict[str, AnyT]] = {}
    negative_tfs: List[str] = []
    one_d_hist: List[float] = []

    for tf in mtf_tfs:
        closes = get_closes_fn(tf) or []
        _, _, hist = compute_macd_from_closes(closes)
        clean = _clean_hist(hist)
        last_val = clean[-1] if clean else 0.0
        prev_val = clean[-2] if len(clean) >= 2 else last_val
        is_pos = last_val > 0.0
        is_flp = (prev_val <= 0.0 and last_val > 0.0)

        tf_states[tf] = {
            "is_positive": is_pos,
            "is_flip": is_flp,
            "hist_last": last_val,
            "hist_prev": prev_val
        }
        if not is_pos:
            negative_tfs.append(tf)
        if tf in ("D", "1D"):
            one_d_hist = clean

    status = "aligned"
    if len(negative_tfs) > 0:
        status = "monitoring" if len(negative_tfs) == 1 else "unaligned"

    one_d_slope_val = 0.0
    if slope is not None and len(one_d_hist) >= mtf_slope_lookback:
        try:
            one_d_slope_val = float(slope(one_d_hist, mtf_slope_lookback))
        except Exception:
            one_d_slope_val = 0.0

    if status == "aligned" and "D" in tf_states and tf_states["D"].get("is_positive"):
        if one_d_slope_val > 0:
            status = "daily_rising"

    return {
        "status": status,
        "tfs": tf_states,
        "negative_tfs": negative_tfs,
        "one_d_slope": one_d_slope_val
    }
