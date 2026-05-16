"""
Bybit REST client updated to use v5 /v5/market/instruments-info per docs screenshot,
and automatically chooses mainnet vs testnet from MAINNET config.

Provides robust _get with rate-limiter, retries, JSON/body logging, and a safe get_symbols.
"""
import time
import hmac
import hashlib
import asyncio
from typing import List, Dict, Any, Optional
import aiohttp

from .config import MAINNET, BYBIT_API_KEY, BYBIT_API_SECRET, RATE_LIMIT_RPS
from .logger import get_logger
from .ratelimiter import TokenBucket

logger = get_logger("bybit_client")


class BybitClient:
    def __init__(self, rate_limiter: Optional[TokenBucket] = None):
        # choose base automatically
        # Accept MAINNET as bool or string ('true'/'false')
        try:
            if isinstance(MAINNET, str):
                mainnet_flag = MAINNET.strip().lower() in ("1", "true", "yes", "y")
            else:
                mainnet_flag = bool(MAINNET)
        except Exception:
            mainnet_flag = True
        if mainnet_flag:
            self.rest_base = "https://api.bybit.com"
        else:
            self.rest_base = "https://api-testnet.bybit.com"
        logger.info("BybitClient using rest_base=%s RATE_LIMIT_RPS=%s", self.rest_base, RATE_LIMIT_RPS)
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self._session: Optional[aiohttp.ClientSession] = None
        self._symbol_info_cache: Dict[str, Dict[str, Any]] = {}
        self._max_retries = 4
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

    async def _get(self, path: str, params: Dict[str, Any] = None, timeout: int = 15):
        session = await self._session_obj()
        url = self.rest_base + path
        for attempt in range(self._max_retries):
            await self.rate_limiter.acquire()
            try:
                async with session.get(url, params=params, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    if status == 429 or (500 <= status < 600):
                        wait = self._backoff_base * (2 ** attempt)
                        logger.warning("HTTP %s from %s, backoff %.1fs (attempt %d/%d)", status, url, wait, attempt + 1, self._max_retries)
                        await asyncio.sleep(wait)
                        continue
                    try:
                        data = await resp.json()
                    except Exception:
                        logger.warning("Non-JSON or unexpected response from %s (status=%s). Body:\n%s", url, status, (text[:2000] + '...') if len(text) > 2000 else text)
                        raise
                    if status >= 400:
                        logger.error("GET %s returned %s: %s", url, status, (text[:400] + "...") if len(text) > 400 else text)
                        raise Exception(f"HTTP {status}")
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt + 1 >= self._max_retries:
                    logger.exception("REST GET failed for %s after %d attempts: %s", url, attempt + 1, e)
                    raise
                wait = self._backoff_base * (2 ** attempt)
                logger.warning("Request error %s; retrying in %.1fs (attempt %d/%d)", e, wait, attempt + 1, self._max_retries)
                await asyncio.sleep(wait)
        raise Exception("unreachable")

    async def get_symbols(self) -> List[Dict[str, Any]]:
        """
        Use v5 instruments-info endpoint (preferred). Fallback to v2/public/symbols.
        v5 path per docs: /v5/market/instruments-info with required param 'category'
        """
        # Try v5 with instruments-info
        try:
            params = {"category": "linear", "instrumentType": "PERPETUAL"}
            data = await self._get("/v5/market/instruments-info", params=params)
            if data and isinstance(data, dict):
                # v5 success shape: ret_code==0 and 'result' may contain 'list' or dict/array
                if data.get("ret_code", 0) == 0 and "result" in data:
                    res = data["result"]
                    # 'result' may be dict with 'list'
                    if isinstance(res, dict) and isinstance(res.get("list"), list):
                        instruments = res.get("list", [])
                    elif isinstance(res, list):
                        instruments = res
                    else:
                        # some v5 shapes return a dict of results
                        instruments = []
                    logger.info("Found %d instruments via v5 instruments-info", len(instruments))
                    return instruments
                # fallback attempt to extract result
                if "result" in data and isinstance(data["result"], (list, dict)):
                    logger.info("Found instruments via v5 (non-standard shape)")
                    return data["result"]
            logger.debug("v5 instruments-info response unexpected: %s", str(data)[:400])
        except Exception as e:
            logger.debug("v5 instruments-info failed: %s", e)

        # Fallback to v2 public symbols
        try:
            data = await self._get("/v2/public/symbols")
            if data and isinstance(data, dict) and "result" in data:
                symbols = data["result"] or []
                logger.info("Found %d symbols via v2/public/symbols", len(symbols))
                return symbols
            logger.debug("v2 symbols response unexpected: %s", str(data)[:800])
        except Exception as e:
            logger.exception("Failed to fetch symbols from v2 endpoint: %s", e)

        return []

    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        sym = symbol.upper()
        if sym in self._symbol_info_cache:
            return self._symbol_info_cache[sym]
        items = await self.get_symbols()
        info: Dict[str, Any] = {"step": None, "min_qty": None, "contract_size": None}
        for it in items:
            if not isinstance(it, dict):
                continue
            # unify name detection across v5/v2
            name = it.get("symbol") or it.get("name") or it.get("instrumentName") or it.get("instrument_name") or it.get("symbolName")
            if not name:
                base = it.get("baseCoin") or it.get("base")
                quote = it.get("quoteCoin") or it.get("quote")
                if base and quote:
                    name = f"{base}{quote}"
            if not name:
                continue
            if name.upper() != sym:
                continue
            # lot/filters parsing (v5/v2 shapes)
            lot = it.get("lotSizeFilter") or it.get("lot_size_filter") or it.get("lot") or {}
            if isinstance(lot, dict):
                step = lot.get("qtyStep") or lot.get("qty_step") or lot.get("stepSize") or lot.get("step")
                min_qty = lot.get("min_trading_qty") or lot.get("minTradingQty") or lot.get("minQty")
                try:
                    if step is not None:
                        info["step"] = float(step)
                    if min_qty is not None:
                        info["min_qty"] = float(min_qty)
                except Exception:
                    pass
            # contract size
            cs = it.get("contractSize") or it.get("contract_size") or it.get("contract")
            try:
                if cs is not None:
                    info["contract_size"] = float(cs)
            except Exception:
                pass
            break
        self._symbol_info_cache[sym] = info
        logger.debug("Symbol info %s => %s", sym, info)
        return info
