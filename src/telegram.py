"""
Async Telegram helper for sending messages and simple health checks.
Uses aiohttp and supports retries + backoff and clear logging of 401 Unauthorized.
"""
import asyncio
import aiohttp
from typing import Optional, Dict, Any
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from .logger import get_logger

logger = get_logger("telegram")

DEFAULT_RETRIES = 3
BACKOFF_BASE = 0.8


class TelegramClient:
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        self.token = token or TELEGRAM_BOT_TOKEN
        self.chat_id = str(chat_id or TELEGRAM_CHAT_ID) if (chat_id or TELEGRAM_CHAT_ID) else None
        self._session: Optional[aiohttp.ClientSession] = None

    async def _session_obj(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _base_url(self) -> str:
        if not self.token:
            raise RuntimeError("Telegram token is not configured")
        return f"https://api.telegram.org/bot{self.token}"

    async def get_me(self) -> Dict[str, Any]:
        """
        Call getMe to validate the bot token. Returns parsed JSON on success.
        """
        session = await self._session_obj()
        url = f"{self._base_url()}/getMe"
        async with session.get(url, timeout=10) as resp:
            j = await resp.json()
            return j

    async def send_message(self, text: str, chat_id: Optional[str] = None, parse_mode: str = "MarkdownV2") -> Dict[str, Any]:
        """
        Send a message to the configured chat id. Retries on transient errors.
        Returns the parsed JSON response on success.
        Raises on persistent failure.
        """
        cid = chat_id or self.chat_id
        if not cid:
            raise RuntimeError("No TELEGRAM_CHAT_ID configured")
        session = await self._session_obj()
        url = f"{self._base_url()}/sendMessage"
        payload = {"chat_id": cid, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        for attempt in range(1, DEFAULT_RETRIES + 1):
            try:
                async with session.post(url, data=payload, timeout=15) as resp:
                    text_body = await resp.text()
                    status = resp.status
                    # parse JSON if available
                    try:
                        j = await resp.json()
                    except Exception:
                        # log the non-json body for diagnosis
                        logger.warning("Telegram non-JSON response status=%s body=%s", status, text_body[:1000])
                        raise
                    if status == 401:
                        # Unauthorized: bad token
                        logger.error("Telegram unauthorized (401). Check TELEGRAM_BOT_TOKEN.")
                        raise Exception("Telegram Unauthorized (401)")
                    if status >= 400:
                        logger.error("Telegram send failed status=%s response=%s", status, j)
                        raise Exception(f"Telegram send failed: {j}")
                    # success
                    logger.info("Telegram message sent to %s", cid)
                    return j
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= DEFAULT_RETRIES:
                    logger.exception("Telegram send failed after %d attempts: %s", attempt, exc)
                    raise
                wait = BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning("Telegram send error, retrying in %.1fs (attempt %d/%d): %s", wait, attempt, DEFAULT_RETRIES, exc)
                await asyncio.sleep(wait)
        raise Exception("unreachable")

# convenience module-level client
_client: Optional[TelegramClient] = None


def get_client() -> TelegramClient:
    global _client
    if _client is None:
        _client = TelegramClient()
    return _client
