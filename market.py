"""
market.py
======================================
Модуль работы с BingX REST API.

Назначение:
- получение рыночных данных
- получение свечей
- получение стакана
- получение funding
- получение open interest
- получение mark price
- подготовка единого MarketSnapshot

Проект:
Hyper AI Trader
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Any
from typing import Tuple

import aiohttp
import orjson

from loguru import logger
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_fixed

from config import Config


# ==========================================================
# MARKET SNAPSHOT
# ==========================================================

@dataclass(slots=True)
class MarketSnapshot:

    symbol: str

    price: float

    mark_price: float

    funding_rate: float

    open_interest: float

    bid_price: float

    ask_price: float

    spread: float

    bid_volume: float

    ask_volume: float

    timestamp: int

    candles_1m: List[dict]

    candles_5m: List[dict]

    candles_15m: List[dict]


# ==========================================================
# CLIENT
# ==========================================================

class BingXMarket:

    """
    Главный клиент BingX.

    Используется только стратегией.

    Strategy никогда не должна знать,
    каким образом получаются данные.
    """

    BASE_URL = "https://open-api.bingx.com"

    def __init__(self):

        self.api_key = Config.API_KEY
        self.secret = Config.API_SECRET

        self.symbol = Config.SYMBOL

        self.timeout = aiohttp.ClientTimeout(total=15)

        self.session: Optional[aiohttp.ClientSession] = None

        self.last_price: float = 0.0

        self.last_snapshot: Optional[MarketSnapshot] = None

        logger.info("Market module initialized")

    # ======================================================

    async def connect(self):

        if self.session is None:

            self.session = aiohttp.ClientSession(
                timeout=self.timeout
            )

            logger.success("HTTP session opened")

    # ======================================================

    async def close(self):

        if self.session:

            await self.session.close()

            logger.info("HTTP session closed")

    # ======================================================

    def _timestamp(self) -> int:

        return int(time.time() * 1000)

    # ======================================================

    def _sign(self, query: str) -> str:

        return hmac.new(
            self.secret.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

    # ======================================================

    def _headers(self):

        return {
            "X-BX-APIKEY": self.api_key
        }

    # ======================================================

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1)
    )
    async def _request(

        self,

        method: str,

        endpoint: str,

        params: Optional[Dict] = None,

        signed: bool = False

    ) -> Dict[str, Any]:

        if self.session is None:

            await self.connect()

        params = params or {}

        if signed:

            params["timestamp"] = self._timestamp()

            query = "&".join(
                f"{k}={v}"
                for k, v in sorted(params.items())
            )

            params["signature"] = self._sign(query)

        url = self.BASE_URL + endpoint

        try:

            async with self.session.request(

                method,

                url,

                params=params,

                headers=self._headers()

            ) as response:

                text = await response.text()

                if response.status != 200:

                    raise Exception(
                        f"{response.status}: {text}"
                    )

                return orjson.loads(text)

        except Exception as e:

            logger.exception(e)

            raise

# ==========================================================
# MARKET DATA METHODS
# ==========================================================

    async def get_price(self) -> float:
        """
        Получает последнюю торговую цену HYPER-USDT.

        Возвращает:
            float: последняя цена.
        """

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v2/quote/price",
            params={
                "symbol": self.symbol
            }
        )

        try:
            price = float(data["data"]["price"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(
                f"Invalid price response from BingX: {data}"
            )
            raise ValueError(
                "Unable to parse current market price"
            ) from exc

        self.last_price = price

        return price

    # ======================================================

    async def get_mark_price(self) -> float:
        """
        Получает mark price фьючерсного контракта.

        Mark Price используется для:
        - расчёта нереализованного PnL;
        - ликвидации;
        - контроля риска.
        """

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v2/quote/premiumIndex",
            params={
                "symbol": self.symbol
            }
        )

        try:
            mark_price = float(
                data["data"]["markPrice"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(
                f"Invalid mark price response: {data}"
            )
            raise ValueError(
                "Unable to parse mark price"
            ) from exc

        return mark_price

    # ======================================================

    async def get_funding_rate(self) -> float:
        """
        Получает текущую ставку funding rate.

        Значение возвращается в десятичном формате.

        Например:

            0.0001 = 0.01%
            -0.0002 = -0.02%
        """

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v2/quote/premiumIndex",
            params={
                "symbol": self.symbol
            }
        )

        try:

            funding_rate = float(
                data["data"]["lastFundingRate"]
            )

        except (KeyError, TypeError, ValueError) as exc:

            logger.error(
                f"Invalid funding response: {data}"
            )

            raise ValueError(
                "Unable to parse funding rate"
            ) from exc

        return funding_rate

    # ======================================================

    async def get_open_interest(self) -> float:
        """
        Получает Open Interest по фьючерсному контракту.

        Возвращает объём открытого интереса.
        """

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v2/quote/openInterest",
            params={
                "symbol": self.symbol
            }
        )

        try:

            open_interest = float(
                data["data"]["openInterest"]
            )

        except (KeyError, TypeError, ValueError) as exc:

            logger.error(
                f"Invalid open interest response: {data}"
            )

            raise ValueError(
                "Unable to parse open interest"
            ) from exc

        return open_interest

    # ======================================================

    async def get_orderbook(
        self,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Получает стакан фьючерсного рынка.

        limit:
            количество уровней стакана.

        Возвращает исходную структуру BingX.
        """

        if limit <= 0:
            raise ValueError(
                "Orderbook limit must be greater than zero"
            )

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v2/quote/depth",
            params={
                "symbol": self.symbol,
                "limit": limit
            }
        )

        if "data" not in data:

            logger.error(
                f"Invalid orderbook response: {data}"
            )

            raise ValueError(
                "Unable to parse orderbook"
            )

        return data["data"]

    # ======================================================

    async def get_best_bid_ask(
        self
    ) -> Tuple[float, float, float, float]:
        """
        Получает лучшие цены Bid / Ask и их объёмы.

        Возвращает:

            bid_price
            ask_price
            bid_volume
            ask_volume
        """

        orderbook = await self.get_orderbook(
            limit=5
        )

        try:

            bids = orderbook["bids"]
            asks = orderbook["asks"]

            if not bids or not asks:
                raise ValueError(
                    "Empty orderbook"
                )

            best_bid = bids[0]
            best_ask = asks[0]

            bid_price = float(best_bid[0])
            bid_volume = float(best_bid[1])

            ask_price = float(best_ask[0])
            ask_volume = float(best_ask[1])

        except (
            KeyError,
            IndexError,
            TypeError,
            ValueError
        ) as exc:

            logger.error(
                f"Invalid orderbook structure: {orderbook}"
            )

            raise ValueError(
                "Unable to parse best bid/ask"
            ) from exc

        return (
            bid_price,
            ask_price,
            bid_volume,
            ask_volume
        )

    # ======================================================

    async def get_spread(self) -> float:
        """
        Возвращает абсолютный Bid/Ask spread.
        """

        bid_price, ask_price, _, _ = (
            await self.get_best_bid_ask()
        )

        return ask_price - bid_price

    # ======================================================

    async def get_spread_percent(self) -> float:
        """
        Возвращает Bid/Ask spread в процентах.
        """

        bid_price, ask_price, _, _ = (
            await self.get_best_bid_ask()
        )

        if bid_price <= 0:
            return 0.0

        return (
            (ask_price - bid_price)
            / bid_price
        ) * 100.0