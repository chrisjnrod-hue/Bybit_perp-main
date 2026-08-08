
# telegram.py
"""
Drop-in async Telegram sender similar to your original implementation,
but resilient and re-uses a session. Returns dicts and never raises.
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
    Send Telegram message. Returns a dict similar to the Telegram API response on success,
    {'skipped': True} when token/chat missing, or {'ok': False, ...} on failure.
    Never raises (except CancelledError).
    """
    token = TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        logger.warning("Telegram not configured (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID). Skipping.")
        return {"skipped": True}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": str(cid),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        session = await _get_session()
        async with session.post(url, data=payload, timeout=timeout) as resp:
            status = resp.status
            text_body = await resp.text()
            try:
                j = await resp.json()
            except Exception:
                logger.warning("Telegram non-JSON response status=%s body=%s", status, text_body[:1000])
                return {"ok": False, "error": "non-json-response", "status": status, "body": text_body[:1000]}
            if status == 401:
                logger.error("Telegram send failed 401: %s", j)
                # return structure similar to your earlier log
                return {"ok": False, "error_code": 401, "description": "Unauthorized", "raw": j}
            if status >= 400:
                logger.warning("Telegram send failed status=%s response=%s", status, j)
                return {"ok": False, "error_code": status, "description": "HTTP error", "raw": j}
            logger.info("Telegram message sent")
            return j
    except asyncio.CancelledError:
        raise
    except Exception as err:
        logger.warning("Telegram send exception: %s", err, exc_info=True)
        return {"ok": False, "error": str(err)}
