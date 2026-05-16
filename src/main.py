import asyncio
import signal
from .logger import get_logger
from .scanner import Scanner
from .ratelimiter import TokenBucket
from .config import RATE_LIMIT_RPS

logger = get_logger("main")

async def main():
    logger.info("Starting bot...")
    # Create a shared rate limiter and pass it into scanner's client
    scanner = Scanner()
    # replace scanner's rate limiter with configured one
    scanner.rate_limiter = TokenBucket(max(1.0, float(RATE_LIMIT_RPS)))
    scanner.client.rate_limiter = scanner.rate_limiter
    # run scanner
    run_task = asyncio.create_task(scanner.run())

    # handle termination signals
    def _cancel(_signame):
        logger.info("Received signal %s, shutting down", _signame)
        scanner.stop()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _cancel, s.name)
        except NotImplementedError:
            # Windows or non-supporting loop
            pass

    try:
        await run_task
    except asyncio.CancelledError:
        logger.info("Main cancelled")
    except Exception:
        logger.exception("Unhandled exception in main")
    finally:
        await scanner.client.close()
        logger.info("Exited.")

if __name__ == "__main__":
    asyncio.run(main())
