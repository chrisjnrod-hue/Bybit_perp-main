"""
Defensive Bybit client using v5 /v5/market endpoints with v2 fallback.
- Automatically picks mainnet/testnet from MAINNET env.
- _get returns parsed JSON dict or None on non-JSON / error (does NOT raise),
  so callers can gracefully fallback and scanner won't exit.
- get_symbols returns list (possibly empty) rather than raising.
- Provides helper wrappers used by scanner: get_klines, get_latest_price, get_symbol_info.
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

    # existing get_symbols unchanged except defensive returns
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
                    # some v5 shapes have result.list
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

    # ---- New convenience wrappers used by Scanner ----

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> Optional[Any]:
        """
        Try v5 /v5/market/kline then v2 /v2/public/kline/list. Return raw payload (list/dict) or None.
        """
        try:
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            data = await self._get("/v5/market/kline", params=params)
            if isinstance(data, dict):
                # v5 typically returns {"ret_code":0,"result":{"list":[...], ...}} or result as list
                if data.get("ret_code", 0) == 0 and "result" in data:
                    res = data["result"]
                    if isinstance(res, dict) and isinstance(res.get("list"), list):
                        return res.get("list", [])
                    if isinstance(res, list):
                        return res
                    # sometimes result itself is the list
                    # if result contains typical kline entries as a list-like, coerce:
                    if isinstance(res, (list, tuple)):
                        return list(res)
                    # otherwise fallthrough and return result
                    return res
            # fallback v2
            data2 = await self._get("/v2/public/kline/list", params={"symbol": symbol, "interval": interval, "limit": limit})
            if isinstance(data2, dict) and "result" in data2:
                return data2["result"]
        except Exception:
            logger.exception("get_klines failed for %s %s", symbol, interval)
        return None

    async def get_latest_price(self, symbol: str) -> Optional[float]:
        """
        Try v5 tickers and v2 tickers to extract last price as float. Returns None if not available.
        """
        try:
            params = {"symbol": symbol}
            data = await self._get("/v5/market/tickers", params=params)
            # v5 often returns {"ret_code":0,"result":[{"symbol":"BTCUSDT","lastPrice":"..."}]}
            if isinstance(data, dict) and "result" in data:
                res = data["result"]
                # if dict with list
                if isinstance(res, list) and len(res) > 0:
                    entry = res[0]
                elif isinstance(res, dict) and "list" in res and isinstance(res["list"], list) and len(res["list"]) > 0:
                    entry = res["list"][0]
                elif isinstance(res, dict):
                    # sometimes result is a dict with symbol keys
                    entry = res
                else:
                    entry = None
                if entry:
                    for k in ("lastPrice", "last_price", "last", "price"):
                        if k in entry and entry[k] is not None:
                            try:
                                return float(entry[k])
                            except Exception:
                                continue
            # fallback to v2 /v2/public/tickers
            data2 = await self._get("/v2/public/tickers", params={"symbol": symbol})
            if isinstance(data2, dict) and "result" in data2:
                res = data2["result"]
                if isinstance(res, list) and len(res) > 0:
                    entry = res[0]
                    if "last_price" in entry:
                        try:
                            return float(entry["last_price"])
                        except Exception:
                            pass
        except Exception:
            logger.exception("get_latest_price error for %s", symbol)
        return None

    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        Attempt to return normalized symbol info. Scanner looks for 'step' and 'min_qty' keys.
        This function searches get_symbols() result for matching symbol name and extracts common fields.
        """
        try:
            syms = await self.get_symbols()
            if not syms:
                return {}
            target = None
            for it in syms:
                try:
                    if isinstance(it, str) and it.upper() == symbol.upper():
                        target = it
                        break
                    if isinstance(it, dict):
                        # many possible name fields
                        name = it.get("name") or it.get("symbol") or it.get("symbolName") or it.get("instrument_name")
                        if name and name.upper() == symbol.upper():
                            target = it
                            break
                        # v5 instruments may use "baseCoin"/"quoteCoin"
                        base = it.get("baseCoin") or it.get("base")
                        quote = it.get("quoteCoin") or it.get("quote")
                        if base and quote and f"{base}{quote}".upper() == symbol.upper():
                            target = it
                            break
                except Exception:
                    continue
            if not target:
                return {}

            info: Dict[str, Any] = {}

            if isinstance(target, dict):
                # try several common paths for step/minQty
                # v5/v2 shapes differ; try lotSizeFilter / qty_step / step
                step = None
                min_qty = None
                # possible nested filters
                for key in ("lotSizeFilter", "lot_size_filter", "qty_filter", "quantity_filter"):
                    filt = target.get(key)
                    if isinstance(filt, dict):
                        step = step or filt.get("qtyStep") or filt.get("step") or filt.get("minQty") or filt.get("minQty")
                        min_qty = min_qty or filt.get("minQty") or filt.get("minQty")
                # flat fields
                step = step or target.get("qty_step") or target.get("step") or target.get("quantity_step") or target.get("tick_size")
                min_qty = min_qty or target.get("min_trading_qty") or target.get("min_qty") or target.get("minOrderQty") or target.get("lot_size")
                # try numeric conversion where possible
                try:
                    if step is not None:
                        step = float(step)
                except Exception:
                    step = None
                try:
                    if min_qty is not None:
                        min_qty = float(min_qty)
                except Exception:
                    min_qty = None

                info["step"] = step
                info["min_qty"] = min_qty

                # attach raw target for caller's use
                info["raw"] = target

            return info
        except Exception:
            logger.exception("get_symbol_info failed for %s", symbol)
            return {}

    async def get_balance(self, currency: str = "USDT") -> Optional[Dict[str, Any]]:
        """
        Placeholder: returns None when no authenticated implementation is available.
        If you want live balance calls, implement signed/private endpoints here.
        """
        if not self.api_key or not self.api_secret:
            logger.debug("get_balance: no api keys configured; returning None")
            return None
        # Authenticated balance fetch is not implemented here to avoid accidental use.
        logger.warning("get_balance: authenticated balance fetch not implemented. Returning None.")
        return None

    async def create_order(self, symbol: str, side: str, qty: float) -> Dict[str, Any]:
        """
        Placeholder for creating an order. This client intentionally does NOT implement signing
        and real order placement to avoid accidental live trades. If you intend to place real orders,
        please implement authenticated POST /private endpoints using your API keys with proper signing.

        Behavior:
        - If api_key/api_secret are not configured, returns a simulated order dict.
        - If api_key/api_secret are configured, raises NotImplementedError to force explicit implementation.
        """
        if not self.api_key or not self.api_secret:
            logger.info("create_order: api keys missing; returning simulated order")
            return {"status": "simulated", "symbol": symbol, "side": side, "qty": qty}
        logger.error("create_order: API keys present but create_order is not implemented for live orders. Implement signing or disable.")
        raise NotImplementedError("create_order is not implemented for live orders in BybitClient. Implement signed order placement.")
