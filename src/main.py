"""
Entrypoint that runs both a small aiohttp health server (binds to $PORT)
and the scanner in the same asyncio event loop. This ensures Render Web
Services sees a listening process and keeps the container alive.
"""
import asyncio
import os
import signal
from aiohttp import web
from .logger import get_logger
from .scanner import Scanner
from .ratelimiter import TokenBucket
from .config import RATE_LIMIT_RPS

logger = get_logger("main")

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
