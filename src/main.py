"""
Entrypoint that runs both a small aiohttp health server (binds to $PORT)
and the scanner in the same asyncio event loop. This ensures Render Web
Services sees a listening process and keeps the container alive.
"""
import asyncio
import os
import signal
import time
import math
from typing import Optional
from aiohttp import web
from .logger import get_logger
from .scanner import Scanner
from .ratelimiter import TokenBucket
from .config import RATE_LIMIT_RPS, SIGNAL_DEDUP_WINDOW

logger = get_logger("main")

# Debug API key env:
# - If DEBUG_API_KEY is not set (None) -> debug endpoint is disabled (returns 404).
# - If DEBUG_API_KEY == "" (empty string) -> anonymous access allowed.
# - If DEBUG_API_KEY == "secret" -> caller must provide ?api_key=secret
DEBUG_API_KEY = os.getenv("DEBUG_API_KEY", None)


async def health(request):
    return web.Response(text="ok")


def _parse_bool(x: Optional[str]) -> bool:
    if x is None:
        return False
    return str(x).strip().lower() in ("1", "true", "yes", "y")


def _dedupe_peek(scanner: Scanner, symbol: str, tf: str, candle_open_time: Optional[int], now: float) -> bool:
    """
    Non-mutating dedupe check: returns True if signal would be considered NEW
    according to the scanner dedupe policy, without modifying the scanner cache.
    Mirrors logic in scanner._try_dedupe_signal but does not write to cache.
    """
    try:
        if SIGNAL_DEDUP_WINDOW <= 0:
            return True
        if candle_open_time is None:
            return True
        key = (symbol, tf, int(candle_open_time))
        last_signal_time = scanner._signal_cache.get(key)
        if last_signal_time is None:
            return True
        time_since_last = now - last_signal_time
        return time_since_last >= float(SIGNAL_DEDUP_WINDOW)
    except Exception:
        return True


async def debug_symbols(request: web.Request):
    """
    Returns a summary of discovered symbols and kline counts cached in the scanner.
    Requires DEBUG_API_KEY to be set (empty-string allows anonymous).
    Usage: GET /debug/symbols?api_key=secret
    """
    if DEBUG_API_KEY is None:
        return web.Response(status=404, text="Debug endpoint disabled. Set DEBUG_API_KEY env to enable ('' for anonymous or a secret).")

    api_key = request.query.get("api_key", "")
    if DEBUG_API_KEY != "" and api_key != DEBUG_API_KEY:
        return web.Response(status=403, text="Forbidden - invalid debug api_key")

    scanner: Scanner = request.app.get("scanner")
    if not scanner:
        return web.Response(status=500, text="Scanner not initialized")

    try:
        result = {}
        for sym in scanner.symbols:
            tf_map = {}
            store = scanner.kline_store.get(sym, {})
            for tf, lst in store.items():
                try:
                    tf_map[str(tf)] = len(lst or [])
                except Exception:
                    tf_map[str(tf)] = 0
            result[sym] = tf_map
    except Exception:
        logger.exception("Failed to build symbol summary for debug_symbols")
        return web.Response(status=500, text="Internal error")

    return web.json_response({"symbols_count": len(scanner.symbols), "symbols": result})


async def debug_seed_symbol(request: web.Request):
    """
    Trigger a background seed_klines_for_symbol(symbol) to populate kline cache.
    Useful when a symbol shows 0 cached candles in the debug view.
    Access control same as other debug endpoints.
    Usage: GET /debug/seed/BTCUSDT?api_key=secret
    """
    if DEBUG_API_KEY is None:
        return web.Response(status=404, text="Debug endpoint disabled. Set DEBUG_API_KEY env to enable ('' for anonymous or a secret).")

    api_key = request.query.get("api_key", "")
    if DEBUG_API_KEY != "" and api_key != DEBUG_API_KEY:
        return web.Response(status=403, text="Forbidden - invalid debug api_key")

    symbol = request.match_info.get("symbol", "").upper()
    if not symbol:
        return web.Response(status=400, text="Missing symbol path parameter")

    scanner: Scanner = request.app.get("scanner")
    if not scanner:
        return web.Response(status=500, text="Scanner not initialized")

    # spawn background seeding so the request returns quickly
    try:
        asyncio.create_task(scanner.seed_klines_for_symbol(symbol))
        return web.json_response({"status": "seeding_started", "symbol": symbol})
    except Exception:
        logger.exception("Failed to start seeding for symbol %s", symbol)
        return web.Response(status=500, text="Failed to start seeding task")


async def debug_symbol(request: web.Request):
    """
    Debug endpoint with enhanced diagnostics for null values:
    GET /debug/symbol/{symbol}?tf=5&include_traces=1&api_key=...
    
    Provides detailed reasons for why fields are null:
    - last_price: null reason (REST failed, WS unavailable, price extraction failed)
    - 24h_volume: null reason (ticker API unavailable, missing field, extraction failed)
    - klines_count: reasons for 0 (seed failed, API returned empty, normalization failed)
    
    - Requires DEBUG_API_KEY to be set in ENV to be enabled.
      * DEBUG_API_KEY == ""  -> anonymous allowed
      * DEBUG_API_KEY == "secret" -> must provide ?api_key=secret
      * DEBUG_API_KEY is None -> endpoint disabled (returns 404)
    """
    # If disabled, return 404 to avoid accidental exposure
    if DEBUG_API_KEY is None:
        return web.Response(status=404, text="Debug endpoint disabled. Set DEBUG_API_KEY env to enable ('' for anonymous or a secret).")

    api_key = request.query.get("api_key", "")
    # enforce key if DEBUG_API_KEY non-empty
    if DEBUG_API_KEY != "" and api_key != DEBUG_API_KEY:
        return web.Response(status=403, text="Forbidden - invalid debug api_key")

    symbol = request.match_info.get("symbol", "").upper()
    if not symbol:
        return web.Response(status=400, text="Missing symbol path parameter")

    tf = request.query.get("tf", "5")
    include_traces = _parse_bool(request.query.get("include_traces", "0"))

    scanner: Scanner = request.app.get("scanner")
    if not scanner:
        return web.Response(status=500, text="Scanner not initialized")

    now = time.time()
    last_price = scanner._last_price_cache.get(symbol)
    klines = scanner.kline_store.get(symbol, {}).get(tf, [])

    # ---- DIAGNOSE LAST PRICE NULL REASONS ----
    last_price_reason = None
    if last_price is None:
        last_price_reason = "REST get_latest_price returned None AND WS fallback unavailable/failed"

    # ---- DIAGNOSE 24H VOLUME NULL REASONS ----
    vol_data = scanner._24h_volumes.get(symbol)
    vol_change = None
    volume_reason = None
    
    if vol_data is None:
        volume_reason = "24h volume never updated – ticker API call failed or not yet attempted"
    else:
        try:
            vol_change = scanner.compute_24h_volume_change(symbol)
            if vol_change is None:
                volume_reason = "Previous and/or current volume values are None – invalid ticker response"
        except Exception as e:
            volume_reason = f"compute_24h_volume_change raised exception: {str(e)[:100]}"

    # ---- DIAGNOSE KLINES NULL/ZERO REASONS ----
    klines_reason = None
    if len(klines) == 0:
        if symbol not in scanner.kline_store or not scanner.kline_store.get(symbol):
            klines_reason = "Symbol not in kline_store – seed_klines_for_symbol never called or failed completely"
        elif tf not in scanner.kline_store.get(symbol, {}):
            klines_reason = f"TF '{tf}' not in store for {symbol} – this TF was not seeded"
        else:
            klines_reason = f"TF '{tf}' seeded but returned 0 valid candles – API returned empty list, null closes, or normalization filtered all"

    # compute macd via scanner wrapper (keeps identical behavior)
    try:
        macd_line, signal_line, hist = scanner.compute_macd_for(symbol, tf, include_price=last_price)
    except Exception as e:
        logger.exception("MACD compute failed in debug endpoint for %s %s", symbol, tf)
        macd_line, signal_line, hist = None, None, None

    # build cleaned hist_list (mirror scanner logic: remove None/NaN/unconvertible)
    hist_list = []
    raw_len = 0
    try:
        if hist is None:
            raw_len = 0
            hist_list = []
        else:
            try:
                iterable = list(hist)
            except Exception:
                iterable = [hist]
            raw_len = len(iterable)
            for x in iterable:
                if x is None:
                    continue
                try:
                    v = float(x)
                    if math.isnan(v):
                        continue
                    hist_list.append(v)
                except Exception:
                    continue
    except Exception:
        hist_list = []

    # flip detection (use scanner helper)
    flip = False
    flip_reason = None
    try:
        flip = bool(scanner.detect_flip_current_open(hist_list, 0.0, symbol=symbol, tf=tf))
        if not flip and len(hist_list) >= 2:
            prev_h = float(hist_list[-2])
            last_h = float(hist_list[-1])
            if prev_h < 0 and last_h > 0:
                flip = True
                flip_reason = "Conservative heuristic: prev_hist < 0 AND last_hist > 0"
        elif flip:
            flip_reason = "Primary detect_flip_current_open returned True"
        else:
            flip_reason = "No flip detected – histogram not crossing from negative to positive"
    except Exception as e:
        flip = False
        flip_reason = f"Flip detection exception: {str(e)[:100]}"

    # last candle start and age
    start_at = None
    try:
        last_candles = scanner.kline_store.get(symbol, {}).get(tf, [])
        if last_candles:
            start_at = int(last_candles[-1].get("start_at") or 0)
    except Exception:
        start_at = None
    candle_age_ok = scanner._is_candle_age_acceptable(start_at, now)
    
    candle_age_reason = None
    if start_at is None:
        candle_age_reason = "No candles in store – cannot determine age"
    elif not candle_age_ok:
        age_sec = now - start_at
        candle_age_reason = f"Candle too old: {age_sec:.0f} sec ago (max allowed: {scanner._config.get('FLIP_CANDLE_AGE_MAX_SEC', 'unknown')} sec)"
    else:
        age_sec = now - start_at
        candle_age_reason = f"Fresh candle: {age_sec:.0f} sec ago"

    # dedupe check (non-mutating peek so debug calls don't mark signals as seen)
    dedupe_ok = _dedupe_peek(scanner, symbol, tf, start_at, now)
    dedupe_reason = None
    if SIGNAL_DEDUP_WINDOW <= 0:
        dedupe_reason = "Deduplication disabled (SIGNAL_DEDUP_WINDOW <= 0)"
    elif start_at is None:
        dedupe_reason = "Cannot dedupe: candle_open_time is None"
    elif dedupe_ok:
        dedupe_reason = "Signal is new or cache expired – would be allowed"
    else:
        cache_key = (symbol, tf, int(start_at))
        last_sig_time = scanner._signal_cache.get(cache_key)
        if last_sig_time:
            time_since = now - last_sig_time
            dedupe_reason = f"Duplicate blocked: {time_since:.0f} sec since last signal (window: {SIGNAL_DEDUP_WINDOW} sec)"
        else:
            dedupe_reason = "Duplicate detected but time info unavailable"

    # tv rating & mtf alignment
    try:
        tv_score, tv_label = scanner.compute_tv_rating(symbol, tf, price=last_price)
    except Exception as e:
        tv_score, tv_label = 0.0, "Error"
    
    try:
        mtf_align = scanner._compute_mtf_alignment(symbol, last_price or 0.0)
    except Exception as e:
        mtf_align = {"status": "error", "tfs": {}, "negative_tfs": []}

    out = {
        "symbol": symbol,
        "tf": tf,
        "now": now,
        "last_price": last_price,
        "last_price_reason": last_price_reason,
        "24h_volume": vol_data,
        "24h_volume_reason": volume_reason,
        "klines_count": len(klines),
        "klines_reason": klines_reason,
        "last_candle_start_at": start_at,
        "candle_age_seconds": (now - start_at) if start_at else None,
        "candle_age_ok": candle_age_ok,
        "candle_age_reason": candle_age_reason,
        "macd": {
            "macd_line": macd_line,
            "signal_line": signal_line,
            "raw_hist_len": raw_len,
            "clean_hist_len": len(hist_list),
            "hist_last2": hist_list[-2:] if len(hist_list) >= 2 else hist_list
        },
        "flip_detected": flip,
        "flip_reason": flip_reason,
        "dedupe_ok": dedupe_ok,
        "dedupe_reason": dedupe_reason,
        "tv_rating": {"score": tv_score, "label": tv_label},
        "mtf_alignment": mtf_align,
        "vol_change": vol_change,
    }

    if include_traces:
        # include recent klines (trimmed for size)
        try:
            out["klines_sample"] = klines[-50:] if klines else []
        except Exception:
            out["klines_sample"] = []

    return web.json_response(out)


async def start_background_tasks(app: web.Application):
    # Create scanner and run it in the background
    scanner = Scanner()
    # Replace scanner rate limiter with one configured from env
    scanner.rate_limiter = TokenBucket(max(1.0, float(RATE_LIMIT_RPS)))
    scanner.client.rate_limiter = scanner.rate_limiter
    app["scanner"] = scanner
    app["scanner_task"] = asyncio.create_task(scanner.run())
    logger.info("Scanner task started")

    # Log registered routes so we can confirm debug route is present
    try:
        routes_info = []
        for r in app.router.routes():
            try:
                # route may have .method and .path attributes
                method = getattr(r, "method", None) or getattr(r, "methods", None) or ""
                path = getattr(r, "path", None) or (getattr(getattr(r, "resource", None), "canonical", None) if getattr(r, "resource", None) else None) or str(r)
                routes_info.append(f"{method} {path}")
            except Exception:
                routes_info.append(str(r))
    except Exception:
        routes_info = [str(r) for r in app.router.routes()]
    logger.info("Registered routes: %s", routes_info)


async def cleanup_background_tasks(app: web.Application):
    scanner: Scanner = app.get("scanner")
    task: asyncio.Task = app.get("scanner_task")
    if scanner:
        scanner.stop()
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            logger.info("Scanner task cancelled")


def make_app():
    app = web.Application()
    app.router.add_get("/", health)
    # add debug routes (registered regardless; behavior controlled by DEBUG_API_KEY)
    app.router.add_get("/debug/symbol/{symbol}", debug_symbol)
    app.router.add_get("/debug/symbols", debug_symbols)
    app.router.add_get("/debug/seed/{symbol}", debug_seed_symbol)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    return app


def run():
    port = int(os.getenv("PORT", os.getenv("RENDER_INTERNAL_PORT", "10000")))
    app = make_app()

    loop = asyncio.get_event_loop()

    # handle signals gracefully
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(app, s)))
        except NotImplementedError:
            # Windows or restricted environments may not support add_signal_handler
            pass

    web.run_app(app, host="0.0.0.0", port=port)


async def shutdown(app: web.Application, sig):
    logger.info("Received signal %s. Shutting down...", sig)
    await app.shutdown()
    await app.cleanup()


if __name__ == "__main__":
    run()
