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