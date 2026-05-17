"""
Defensive Bybit client using v5 /v5/market/instruments-info (per your docs) with v2 fallback.
- Automatically picks mainnet/testnet from MAINNET env.
- _get returns parsed JSON dict or None on non-JSON / error (does NOT raise),
  so callers can gracefully fallback and scanner won't exit.
- get_symbols returns list (possibly empty) rather than raising.
"""
import asyncio
from typing import List, Dict, Any, Optional
import aiohttp
from .config import MAINNET, BYBIT_API_KEY, BYBIT_API_SECRET, RATE_LIMIT_RPS
from .logger import get_logger
from .ratelimiter import TokenBucket

logger = get_logger("bybit_client")


class BybitClient:
    def __init__(self, rate_limiter: Optional[TokenBucket] = None):
        try:
            if isinstance(MAINNET, str):
                mainnet_flag = MAINNET.strip().lower() in ("1", "true", "yes", "y")
            else:
                mainnet_flag = bool(MAINNET)
        except Exception:
            mainnet_flag = True
        self.rest_base = "https://api.bybit.com" if mainnet_flag else "https://api-testnet.bybit.com"
        logger.info("BybitClient rest_base=%s", self.rest_base)

        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self._session: Optional[aiohttp.ClientSession] = None
        self._max_retries = 3
        self._backoff_base = 1.0
        try:
            rate_val = float(RATE_LIMIT_RPS)
        except Exception:
            rate_val = 5.0
        self.rate_limiter = rate_limiter or TokenBucket(max(1.0, rate_val))

    async def _session_obj(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 12) -> Optional[Dict[str, Any]]:
        """
        Return parsed JSON dict on success, or None on failure/non-JSON so callers can fallback.
        """
        session = await self._session_obj()
        url = self.rest_base + path
        for attempt in range(self._max_retries):
            await self.rate_limiter.acquire()
            try:
                async with session.get(url, params=params, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    # retry on rate-limit/server errors
                    if status == 429 or (500 <= status < 600):
                        wait = self._backoff_base * (2 ** attempt)
                        logger.warning("HTTP %s from %s — backoff %.1fs (attempt %d/%d)", status, url, wait, attempt + 1, self._max_retries)
                        await asyncio.sleep(wait)
                        continue
                    try:
                        data = await resp.json()
                    except Exception:
                        snippet = (text[:2000] + '...') if len(text) > 2000 else text
                        logger.warning("Bybit returned non-JSON (status=%s) from %s. Body:\n%s", status, url, snippet)
                        return None
                    if status >= 400:
                        snippet = (text[:400] + '...') if len(text) > 400 else text
                        logger.error("Bybit GET %s returned %s: %s", url, status, snippet)
                        return None
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt + 1 >= self._max_retries:
                    logger.exception("GET %s failed after %d attempts: %s", url, attempt + 1, e)
                    return None
                wait = self._backoff_base * (2 ** attempt)
                logger.warning("Request error for %s: %s. Retrying in %.1fs (attempt %d/%d)", url, e, wait, attempt + 1, self._max_retries)
                await asyncio.sleep(wait)
        return None

    async def get_symbols(self) -> List[Dict[str, Any]]:
        """
        Try v5 instruments-info first, then fallback to v2 public symbols. Return list (or empty list).
        """
        # try v5 /v5/market/instruments-info
        try:
            params = {"category": "linear", "instrumentType": "PERPETUAL"}
            data = await self._get("/v5/market/instruments-info", params=params)
            if isinstance(data, dict):
                # expected v5 success shape
                if data.get("ret_code", 0) == 0 and "result" in data:
                    res = data["result"]
                    if isinstance(res, dict) and isinstance(res.get("list"), list):
                        instruments = res.get("list", [])
                        logger.info("Found %d instruments via v5", len(instruments))
                        return instruments
                    if isinstance(res, list):
                        logger.info("Found %d instruments via v5", len(res))
                        return res
                # fallback if result exists but unexpected shape
                if "result" in data and isinstance(data["result"], (list, dict)):
                    logger.info("Found instruments via v5 (non-standard shape)")
                    return data["result"] if isinstance(data["result"], list) else list(data["result"])  # safe coercion
        except Exception as e:
            logger.debug("v5 instruments-info attempt failed: %s", e)

        # fallback to v2 /v2/public/symbols
        try:
            data = await self._get("/v2/public/symbols")
            if isinstance(data, dict) and "result" in data:
                symbols = data["result"] or []
                logger.info("Found %d symbols via v2", len(symbols))
                return symbols
            logger.debug("v2 symbols returned unexpected payload.")
        except Exception as e:
            logger.debug("v2 symbols attempt failed: %s", e)

        logger.warning("No symbols retrieved from Bybit; returning empty list.")
        return []
