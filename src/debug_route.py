from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import time

app = FastAPI()

# Replace this check with your preferred auth (env token, IP whitelist, etc.)
DEBUG_API_KEY = "changeme"  # set via env in production!

# Example quick JSON schema using Pydantic isn't required but shown for clarity
class DebugParams(BaseModel):
    tf: str = "5"
    include_traces: bool = False

# scanner_instance should be the Scanner object created in your app.
# This snippet assumes you have access to it (e.g., imported or set on startup).
scanner = None  # set to your Scanner() instance at runtime

@app.get("/debug/symbol/{symbol}")
async def debug_symbol(symbol: str, request: Request, tf: str = "5", include_traces: bool = False, api_key: str = ""):
    # Basic auth check
    if api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    if scanner is None:
        raise HTTPException(status_code=500, detail="scanner not initialized")

    now = time.time()
    # last price
    last_price = scanner._last_price_cache.get(symbol)
    # klines used for MACD
    klines = scanner.kline_store.get(symbol, {}).get(tf, [])
    # compute macd (this returns macd_line, signal_line, hist)
    macd_line, signal_line, hist = scanner.compute_macd_for(symbol, tf, include_price=last_price)
    # build cleaned hist_list (mirrors scanner logic)
    hist_list = []
    try:
        if hist is not None:
            try:
                iterable = list(hist)
            except Exception:
                iterable = [hist]
            for x in iterable:
                if x is None:
                    continue
                try:
                    v = float(x)
                    if not (v != v):  # skip NaN
                        hist_list.append(v)
                except Exception:
                    continue
    except Exception:
        hist_list = []

    # flip detection
    flip = False
    try:
        flip = bool(scanner.detect_flip_current_open(hist_list, 0.0, symbol=symbol, tf=tf))
    except Exception:
        flip = False

    # start_at and candle age
    start_at = None
    try:
        last_candles = scanner.kline_store.get(symbol, {}).get(tf, [])
        if last_candles:
            start_at = int(last_candles[-1].get("start_at") or 0)
    except Exception:
        start_at = None
    candle_age_ok = scanner._is_candle_age_acceptable(start_at, now)

    # dedupe status: return whether this (symbol,tf,start_at) would be accepted now
    dedupe_ok = scanner._try_dedupe_signal(symbol, tf, start_at, now)

    # tv rating, mtf alignment, vol change
    tv_score, tv_label = scanner.compute_tv_rating(symbol, tf, price=last_price)
    mtf_align = scanner._compute_mtf_alignment(symbol, last_price or 0.0)
    vol_change = scanner.compute_24h_volume_change(symbol)

    return {
        "symbol": symbol,
        "tf": tf,
        "now": now,
        "last_price": last_price,
        "24h_volume": scanner._24h_volumes.get(symbol),
        "klines_count": len(klines),
        "last_candle_start_at": start_at,
        "candle_age_seconds": (now - start_at) if start_at else None,
        "candle_age_ok": candle_age_ok,
        "macd": {
            "macd_line": macd_line,
            "signal_line": signal_line,
            "raw_hist_len": len(hist) if hasattr(hist, "__len__") else (1 if hist is not None else 0),
            "clean_hist_len": len(hist_list),
            "hist_last2": hist_list[-2:] if len(hist_list) >= 2 else hist_list
        },
        "flip_detected": flip,
        "dedupe_ok": dedupe_ok,
        "tv_rating": {"score": tv_score, "label": tv_label},
        "mtf_alignment": mtf_align,
        "vol_change": vol_change,
        "signal_cache_keys": bool(scanner._signal_cache.get((symbol, tf, start_at)))  # whether already cached
    }
