"""
Bybit REST client with robust GET handling, v5/v2 symbol fallbacks,
rate-limited requests, and retries/backoff.

Ready-to-paste file: src/bybit_client.py
"""
import time
import hmac
import hashlib
from typing import List, Dict, Any, Optional
import asyncio
import aiohttp
from .config import MAINNET, BYBIT_API_KEY, BYBIT_API_SECRET, RATE_LIMIT_RPS
from .logger import get_logger
from .ratelimiter import TokenBucket

logger = get_logger("bybit")


class BybitClient:
    def __init__(self, rate_limiter: Optional[TokenBucket] = None):
        if MAINNET:
            self.rest_base = "https://api.bybit.com"
        else:
            self.rest_base = "https://api-testnet.bybit.com"
        self.api_key = BYBIT_API_KEY
        self.api_secret = BYBIT_API_SECRET
        self._session: Optional[aiohttp.ClientSession] = None
        self._symbol_info_cache: Dict[str, Dict[str, Any]] = {}
        # request retry/backoff config
        self._max_retries = 4
        self._backoff_base = 1.0  # seconds
        # rate limiter: if not provided, create one from RATE_LIMIT_RPS
        try:
            rate_val = float(RATE_LIMIT_RPS)
        except Exception:
            rate_val = 5.0
        self.rate_limiter = rate_limiter or TokenBucket(max(1.0, rate_val))

    async def _session_obj(self):
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: Dict[str, Any] = None, timeout=15):
        """
        Robust GET with rate-limiting, retries, backoff, and detailed logging on non-JSON bodies.
        """
        session = await self._session_obj()
        url = self.rest_base + path
        for attempt in range(self._max_retries):
            # acquire rate token
            await self.rate_limiter.acquire()
            try:
                async with session.get(url, params=params, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    # handle rate limit / server errors with backoff
                    if status == 429 or (500 <= status < 600):
                        wait = self._backoff_base * (2 ** attempt)
                        logger.warning("HTTP %s from %s, backoff %.1fs (attempt %d/%d)", status, url, wait, attempt + 1, self._max_retries)
                        await asyncio.sleep(wait)
                        continue
                    # Attempt JSON decode but on failure log full body for debugging
                    try:
                        data = await resp.json()
                    except Exception:
                        logger.warning("Non-JSON or unexpected response from %s (status=%s). Body:\n%s",
                                       url, status, (text[:2000] + '...') if len(text) > 2000 else text)
                        # Raise so caller can decide fallback behavior
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
        Return list of instruments/symbols.
        Try v5 first (preferred), then fallback to v2.
        """
        # Try v5 endpoint using params (don't embed querystring in path)
        try:
            params = {"category": "linear", "instrumentType": "PERPETUAL"}
            data = await self._get("/v5/market/instruments", params=params)
            if data and isinstance(data, dict):
                # some v5 responses have ret_code / result
                if data.get("ret_code", 0) == 0 and "result" in data:
                    res = data["result"]
                    # result can be dict with 'list' or a list directly
                    if isinstance(res, dict) and isinstance(res.get("list"), list):
                        instruments = res.get("list", [])
                    elif isinstance(res, list):
                        instruments = res
                    else:
                        instruments = []
                    logger.info("Found %d instruments via v5", len(instruments))
                    return instruments
                # sometimes BYBIT returns different success shape, try to extract 'result' anyway
                if "result" in data and isinstance(data["result"], (list, dict)):
                    logger.info("Found instruments via v5 (non-standard shape)")
                    return data["result"]
            # If data is None or unexpected, log and fall through to v2
            logger.debug("v5 instruments response unexpected: %s", str(data)[:400])
        except Exception as e:
            logger.debug("v5 instruments endpoint failed: %s", e)

        # Fallback to v2
        try:
            data = await self._get("/v2/public/symbols")
            if data and isinstance(data, dict) and "result" in data:
                symbols = data["result"] or []
                logger.info("Found %d symbols via v2", len(symbols))
                return symbols
            # If the response is not as expected, log the raw response for debugging
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
            name = it.get("name") or it.get("symbol") or (it.get("baseCoin") and it.get("quoteCoin") and f"{it.get('baseCoin')}{it.get('quoteCoin')}")
            if not name:
                name = it.get("instrument_name") or it.get("symbolName")
            if not name:
                continue
            if name.upper() != sym:
                continue
            # lot/filters parsing
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
            # v2 style
            if info.get("step") is None:
                filters = it.get("lot_size_filter") or {}
                if isinstance(filters, dict):
                    step = filters.get("qty_step") or filters.get("stepSize")
                    min_qty = filters.get("min_trading_qty") or filters.get("minQty")
                    try:
                        if step is not None:
                            info["step"] = float(step)
                        if min_qty is not None:
                            info["min_qty"] = float(min_qty)
                    except Exception:
                        pass
            if info.get("step") is None:
                for k in ("qty_step", "stepSize", "step"):
                    v = it.get(k)
                    if v:
                        try:
                            info["step"] = float(v)
                            break
                        except Exception:
                            pass
            if info.get("min_qty") is None:
                for k in ("min_trading_qty", "minQty"):
                    v = it.get(k)
                    if v:
                        try:
                            info["min_qty"] = float(v)
                            break
                        except Exception:
                            pass
            # contract size
            if info.get("contract_size") is None:
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

    async def get_klines(self, symbol: str, interval: str, limit: int = 200) -> List[Dict[str, Any]]:
        params = {"symbol": symbol, "interval": self._interval_to_minutes(interval), "limit": limit}
        try:
            data = await self._get("/v2/public/kline/list", params=params)
            if data and isinstance(data, dict) and "result" in data:
                res = data["result"] or []
                out = []
                for r in res:
                    if isinstance(r, dict):
                        out.append({
                            "open": float(r.get("open", 0)),
                            "high": float(r.get("high", 0)),
                            "low": float(r.get("low", 0)),
                            "close": float(r.get("close", 0)),
                            "volume": float(r.get("volume", 0)),
                            "start_at": int(r.get("start_at", 0))
                        })
                    elif isinstance(r, (list, tuple)) and len(r) >= 6:
                        out.append({
                            "open": float(r[0]),
                            "high": float(r[1]),
                            "low": float(r[2]),
                            "close": float(r[3]),
                            "volume": float(r[4]),
                            "start_at": int(r[5])
                        })
                return out
        except Exception:
            logger.exception("Failed v2 kline, trying v5 fallback")
        try:
            params = {"category": "linear", "symbol": symbol, "interval": self._interval_to_minutes(interval), "limit": limit}
            data = await self._get("/v5/market/kline", params=params)
            if data and isinstance(data, dict) and data.get("ret_code", 0) == 0 and "result" in data:
                res = data["result"]
                items = []
                if isinstance(res, dict) and isinstance(res.get("list"), list):
                    items = res.get("list", [])
                elif isinstance(res, list):
                    items = res
                out = []
                for r in items:
                    if isinstance(r, (list, tuple)) and len(r) >= 6:
                        out.append({
                            "start_at": int(r[0]),
                            "open": float(r[1]),
                            "high": float(r[2]),
                            "low": float(r[3]),
                            "close": float(r[4]),
                            "volume": float(r[5])
                        })
                return out
        except Exception:
            logger.exception("v5 kline fallback failed.")
        return []

    def _interval_to_minutes(self, tf: str) -> str:
        tf = tf.strip().lower()
        mapping = {
            "1m": "1", "5m": "5", "15m": "15", "30m": "30",
            "1h": "60", "2h": "120", "4h": "240", "1d": "D",
        }
        return mapping.get(tf, tf)

    async def get_latest_price(self, symbol: str) -> Optional[float]:
        try:
            data = await self._get("/v2/public/tickers", params={"symbol": symbol})
            if data and isinstance(data, dict) and "result" in data:
                rlist = data["result"] or []
                if isinstance(rlist, list) and len(rlist) > 0:
                    r = rlist[0]
                    price = float(r.get("last_price") or r.get("last_tick") or r.get("close", 0))
                    return price
        except Exception:
            logger.debug("v2 ticker failed, trying v5")
        try:
            data = await self._get("/v5/market/ticker", params={"category": "linear", "symbol": symbol})
            if data and isinstance(data, dict) and data.get("ret_code", 0) == 0 and "result" in data:
                r = data["result"]
                if isinstance(r, dict):
                    return float(r.get("last_price", 0) or r.get("last_tx_price", 0) or 0)
        except Exception:
            logger.exception("Ticker fetch failed.")
        return None

    def _sign_v2(self, params: dict) -> str:
        ordered = "&".join([f"{k}={params[k]}" for k in sorted(params) if params[k] is not None])
        return hmac.new(self.api_secret.encode(), ordered.encode(), hashlib.sha256).hexdigest()

    async def get_balance(self, coin: str = "USDT") -> Optional[float]:
        if not self.api_key or not self.api_secret:
            logger.warning("API keys missing: cannot fetch balance")
            return None
        path = "/v2/private/wallet/balance"
        ts = int(time.time() * 1000)
        params = {
            "api_key": self.api_key,
            "timestamp": ts,
            "coin": coin
        }
        params["sign"] = self._sign_v2(params)
        session = await self._session_obj()
        url = self.rest_base + path
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                j = await resp.json()
                if resp.status >= 400 or j.get("ret_code", 0) != 0:
                    logger.error("Balance error %s", j)
                    return None
                res = j.get("result", {})
                if isinstance(res, dict) and coin in res:
                    coindata = res[coin] or {}
                    bal = coindata.get("available_balance") if coindata else None
                    if bal is None:
                        bal = coindata.get("wallet_balance") if coindata else None
                    if bal is not None:
                        return float(bal)
        except Exception:
            logger.exception("Balance fetch failed.")
        return None

    async def create_order(self, symbol: str, side: str, qty: float, price: Optional[float] = None, order_type: str = "Market"):
        if not self.api_key or not self.api_secret:
            logger.warning("API keys missing: simulate order %s %s %s", symbol, side, qty)
            return {"simulated": True, "symbol": symbol, "side": side, "qty": qty, "price": price}
        path = "/v2/private/order/create"
        ts = int(time.time() * 1000)
        params = {
            "api_key": self.api_key,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "qty": qty,
            "time_in_force": "GoodTillCancel",
            "timestamp": ts
        }
        if price is not None:
            params["price"] = price
        params["sign"] = self._sign_v2(params)
        session = await self._session_obj()
        url = self.rest_base + path
        for attempt in range(self._max_retries):
            # rate-limit before sending order
            await self.rate_limiter.acquire()
            try:
                async with session.post(url, data=params, timeout=15) as resp:
                    text = await resp.text()
                    status = resp.status
                    if status == 429 or (500 <= status < 600):
                        wait = self._backoff_base * (2 ** attempt)
                        logger.warning("Order HTTP %s, backoff %.1fs (attempt %d/%d)", status, wait, attempt + 1, self._max_retries)
                        await asyncio.sleep(wait)
                        continue
                    try:
                        j = await resp.json()
                    except Exception:
                        logger.error("Order response non-json: %s", text[:400])
                        raise
                    if status >= 400 or j.get("ret_code", 0) != 0:
                        logger.error("Order error: %s", j)
                        raise Exception("Order failed: " + str(j))
                    logger.info("Order placed: %s", j)
                    return j
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt + 1 >= self._max_retries:
                    logger.exception("Order create failed after attempts.")
                    raise
                wait = self._backoff_base * (2 ** attempt)
                logger.warning("Order create error, retrying in %.1fs", wait)
                await asyncio.sleep(wait)
        raise Exception("unreachable")
