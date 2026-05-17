"""
Drop-in async Telegram sender ported from your old JS sendTelegram().
- Uses HTML parse_mode (same as the JS version).
- Returns dict similar to requests JSON on success.
- Returns {'skipped': True} when token/chat missing.
- Retries/transient handling kept minimal to match the simple JS behavior.
"""
import asyncio
import aiohttp
from typing import Optional, Dict, Any
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .logger import get_logger

logger = get_logger("telegram")

# keep behavior similar to JS: single POST, report errors, don't throw for missing config
async def send_message(text: str, chat_id: Optional[str] = None, timeout: int = 10) -> Dict[str, Any]:
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

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=timeout) as resp:
                status = resp.status
                text_body = await resp.text()
                try:
                    j = await resp.json()
                except Exception:
                    # preserve old behavior: log warning and return error shape
                    logger.warning("Telegram non-JSON response status=%s body=%s", status, text_body[:1000])
                    return {"error": "non-json-response", "status": status, "body": text_body[:1000]}
                if status == 401:
                    logger.error("Telegram send failed 401: %s", j)
                    return {"ok": False, "error_code": 401, "description": "Unauthorized", "raw": j}
                if status >= 400:
                    logger.warning("Telegram send failed status=%s response=%s", status, j)
                    return {"ok": False, "error_code": status, "description": "HTTP error", "raw": j}
                logger.info("Telegram message sent")
                return j
    except asyncio.CancelledError:
        raise
    except Exception as err:
        logger.warning("Telegram send error: %s", str(err))
        return {"error": str(err)}
