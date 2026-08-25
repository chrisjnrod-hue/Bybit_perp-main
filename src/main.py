"""
Entrypoint that runs both a small aiohttp health server (binds to $PORT)
and the scanner in the same asyncio event loop. This ensures Render Web
Services sees a listening process and keeps the container alive.
"""
import asyncio
import os
import signal
import time
from aiohttp import web
from .logger import get_logger
from .scanner import Scanner
from .ratelimiter import TokenBucket
from .config import RATE_LIMIT_RPS

logger = get_logger("main")

# Debug API key env:
# - If DEBUG_API_KEY is not set (None) -> debug endpoint is disabled (returns 404).
# - If DEBUG_API_KEY == "" (empty string) -> anonymous access allowed.
# - If DEBUG_API_KEY == "secret" -> caller must provide ?api_key=secret
DEBUG_API_KEY = os.getenv("DEBUG_API_KEY", None)


async def health(request):
    return web.Response(text="ok")


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
        routes = [r.resource.canonical for r in app.router.routes()]
    except Exception:
        # older aiohttp versions: fallback
        routes = [str(r) for r in app.router.routes()]
    logger.info("Registered routes: %s", routes)


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


def _parse_bool(x: str) -> bool:
    if x is None:
        return False
    return str(x).strip().lower() in ("1", "true", "yes", "y")


async def debug_symbol(request: web.Request):
    """
    Debug endpoint:
    GET /debug/symbol/{symbol}?tf=5&include_traces=1&api_key=...
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
    try:
        flip = bool(scanner.detect_flip_current_open(hist_list, 0.0, symbol=symbol, tf=tf))
    except Exception:
        flip = False

    # last candle start and age
    start_at = None
    try:
        last_candles = scanner.kline_store.get(symbol, {}).get(tf, [])
        if last_candles:
            start_at = int(last_candles[-1].get("start_at") or 0)
    except Exception:
        start_at = None
    candle_age_ok = scanner._is_candle_age_acceptable(start_at, now)

    # dedupe check (do not update cache; call will update cache in scanner, but that's acceptable for debug)
    dedupe_ok = scanner._try_dedupe_signal(symbol, tf, start_at, now)

    # tv rating & mtf alignment & vol change
    try:
        tv_score, tv_label = scanner.compute_tv_rating(symbol, tf, price=last_price)
    except Exception:
        tv_score, tv_label = 0.0, "Neutral"
    try:
        mtf_align = scanner._compute_mtf_alignment(symbol, last_price or 0.0)
    except Exception:
        mtf_align = {"status": "error", "tfs": {}, "negative_tfs": []}
    vol_change = scanner.compute_24h_volume_change(symbol)

    out = {
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
            "raw_hist_len": raw_len,
            "clean_hist_len": len(hist_list),
            "hist_last2": hist_list[-2:] if len(hist_list) >= 2 else hist_list
        },
        "flip_detected": flip,
        "dedupe_ok": dedupe_ok,
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


def make_app():
    app = web.Application()
    app.router.add_get("/", health)
    # add debug route (registered regardless; behavior controlled by DEBUG_API_KEY)
    app.router.add_get("/debug/symbol/{symbol}", debug_symbol)
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
