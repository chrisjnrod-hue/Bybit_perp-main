# telegram.py
"""
Drop-in async Telegram sender similar to your original implementation,
but resilient, re-uses a session, and provides improved diagnostic logging.
Returns dict-like responses and never raises (except CancelledError).
"""
import asyncio
import aiohttp
from typing import Optional, Dict, Any
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .logger import get_logger

logger = get_logger("telegram")

# Reuse session across calls to avoid overhead
_session: Optional[aiohttp.ClientSession] = None

async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session

async def close_session() -> None:
    global _session
    if _session:
        try:
            await _session.close()
        except Exception:
            logger.exception("Error closing Telegram session")
    _session = None

async def send_message(text: str, chat_id: Optional[str] = None, timeout: int = 10) -> Dict[str, Any]:
    """
    Send a Telegram message.

    Returns:
      - {"skipped": True} when TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing.
      - The parsed JSON response from Telegram on success (usually contains "ok": True).
      - {"ok": False, ...} on HTTP/parse/other errors with diagnostic fields.

    This function:
      - Reuses an aiohttp.ClientSession across calls.
      - Never raises (except asyncio.CancelledError).
      - Logs detailed diagnostics to help debug delivery failures.
    """
    token = TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        logger.warning("Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID). Skipping send.")
        return {"skipped": True}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": str(cid),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    # Log a debug preview of the payload (do not log full secret token)
    try:
        logger.debug("Telegram send payload: chat_id=%s text_len=%d", str(cid), len(text))
    except Exception:
        logger.debug("Telegram send payload: (unable to compute payload preview)")

    try:
        session = await _get_session()
        async with session.post(url, data=payload, timeout=timeout) as resp:
            status = resp.status
            text_body = await resp.text()
            # Try to parse JSON; if that fails, return structured error with truncated body
            try:
                j = await resp.json()
            except Exception:
                logger.warning("Telegram non-JSON response status=%s body=%s", status, text_body[:1000])
                return {"ok": False, "error": "non-json-response", "status": status, "body": text_body[:1000]}

            # Always log the parsed response at debug level for diagnostics
            try:
                logger.debug("Telegram HTTP status=%s response=%s", status, j)
            except Exception:
                logger.debug("Telegram HTTP status=%s response=(unprintable JSON)", status)

            # Authorization error
            if status == 401:
                logger.error("Telegram send failed 401 Unauthorized: %s", j)
                return {"ok": False, "error_code": 401, "description": "Unauthorized", "raw": j}

            # Other HTTP errors
            if status >= 400:
                logger.warning("Telegram send failed status=%s response=%s", status, j)
                return {"ok": False, "error_code": status, "description": "HTTP error", "raw": j}

            # Success
            logger.info("Telegram message sent (chat_id=%s text_len=%d)", str(cid), len(text))
            logger.debug("Telegram send success payload_preview=%s response_ok=%s", (text[:200] + "..." if len(text) > 200 else text), j.get("ok", True))
            return j
    except asyncio.CancelledError:
        # Preserve cancellation semantics for caller
        raise
    except Exception as err:
        logger.warning("Telegram send exception: %s", err, exc_info=True)
        return {"ok": False, "error": str(err)}
