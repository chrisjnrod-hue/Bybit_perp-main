
import asyncio
import aiohttp
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .logger import get_logger

logger = get_logger("telegram")
API_BASE = "https://api.telegram.org"

async def send_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram not configured, message skipped.")
        logger.debug("Telegram message would be:\n%s", text)
        return
    url = f"{API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    text_resp = await resp.text()
                    logger.error("Telegram send failed %s: %s", resp.status, text_resp)
                else:
                    logger.info("Telegram sent message (%d chars)", len(text))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Telegram send error: %s", e)
