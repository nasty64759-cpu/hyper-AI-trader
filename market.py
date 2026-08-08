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
        
# ==========================================================
# CANDLES
# ==========================================================

    async def get_candles(
        self,
        interval: str = "1m",
        limit: int = 200
    ) -> List[dict]:
        """
        Получает свечи фьючерсного рынка BingX.

        interval:
            1m, 3m, 5m, 15m, 30m,
            1h, 2h, 4h, 6h, 12h, 1d и т.д.

        limit:
            количество свечей.

        Возвращает список свечей в нормализованном формате:
        {
            "timestamp": ...,
            "open": ...,
            "high": ...,
            "low": ...,
            "close": ...,
            "volume": ...
        }
        """

        if limit <= 0:
            raise ValueError(
                "Candle limit must be greater than zero"
            )

        if limit > 1000:
            limit = 1000

        data = await self._request(
            method="GET",
            endpoint="/openApi/swap/v3/quote/klines",
            params={
                "symbol": self.symbol,
                "interval": interval,
                "limit": limit
            }
        )

        raw_candles = data.get("data")

        if not raw_candles:
            logger.error(
                f"Empty candle response: {data}"
            )
            return []

        candles = []

        try:

            for candle in raw_candles:

                # BingX может возвращать свечу
                # в виде списка либо объекта.

                if isinstance(candle, dict):

                    timestamp = int(
                        candle.get(
                            "time",
                            candle.get("timestamp", 0)
                        )
                    )

                    open_price = float(
                        candle["open"]
                    )

                    high_price = float(
                        candle["high"]
                    )

                    low_price = float(
                        candle["low"]
                    )

                    close_price = float(
                        candle["close"]
                    )

                    volume = float(
                        candle.get("volume", 0)
                    )

                else:

                    timestamp = int(candle[0])

                    open_price = float(candle[1])
                    high_price = float(candle[2])
                    low_price = float(candle[3])
                    close_price = float(candle[4])
                    volume = float(candle[5])

                candles.append(
                    {
                        "timestamp": timestamp,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume
                    }
                )

        except (
            KeyError,
            IndexError,
            TypeError,
            ValueError
        ) as exc:

            logger.error(
                f"Unable to parse candles: {data}"
            )

            raise ValueError(
                "Unable to parse BingX candle data"
            ) from exc

        # Старые свечи → новые свечи.
        candles.sort(
            key=lambda candle: candle["timestamp"]
        )

        return candles

    # ======================================================

    async def get_1m_candles(
        self,
        limit: int = 200
    ) -> List[dict]:
        """
        Получает минутные свечи.
        """

        return await self.get_candles(
            interval="1m",
            limit=limit
        )

    # ======================================================

    async def get_5m_candles(
        self,
        limit: int = 200
    ) -> List[dict]:
        """
        Получает 5-минутные свечи.
        """

        return await self.get_candles(
            interval="5m",
            limit=limit
        )

    # ======================================================

    async def get_15m_candles(
        self,
        limit: int = 200
    ) -> List[dict]:
        """
        Получает 15-минутные свечи.
        """

        return await self.get_candles(
            interval="15m",
            limit=limit
        )


# ==========================================================
# MARKET SNAPSHOT
# ==========================================================

    async def get_market_snapshot(
        self,
        candle_limit: int = 200
    ) -> MarketSnapshot:
        """
        Получает полный снимок рынка.

        В одном объекте собираются:

        - текущая цена;
        - Mark Price;
        - Funding Rate;
        - Open Interest;
        - Bid;
        - Ask;
        - объёмы Bid/Ask;
        - Spread;
        - свечи 1m;
        - свечи 5m;
        - свечи 15m.

        Этот объект будет основным источником
        данных для indicators.py и strategy.py.
        """

        logger.debug(
            f"Updating market snapshot: {self.symbol}"
        )

        (
            price,
            mark_price,
            funding_rate,
            open_interest,
            best_bid,
            best_ask,
            bid_volume,
            ask_volume,
            candles_1m,
            candles_5m,
            candles_15m
        ) = await asyncio.gather(

            self.get_price(),

            self.get_mark_price(),

            self.get_funding_rate(),

            self.get_open_interest(),

            self.get_best_bid_ask(),

            self.get_1m_candles(
                limit=candle_limit
            ),

            self.get_5m_candles(
                limit=candle_limit
            ),

            self.get_15m_candles(
                limit=candle_limit
            )
        )

        # get_best_bid_ask() возвращает кортеж.
        #
        # Поэтому asyncio.gather() вернул его
        # как один отдельный элемент.
        #
        # Распакуем его здесь.

        (
            best_bid,
            best_ask,
            bid_volume,
            ask_volume
        ) = best_bid

        spread = best_ask - best_bid

        snapshot = MarketSnapshot(

            symbol=self.symbol,

            price=price,

            mark_price=mark_price,

            funding_rate=funding_rate,

            open_interest=open_interest,

            bid_price=best_bid,

            ask_price=best_ask,

            spread=spread,

            bid_volume=bid_volume,

            ask_volume=ask_volume,

            timestamp=self._timestamp(),

            candles_1m=candles_1m,

            candles_5m=candles_5m,

            candles_15m=candles_15m
        )

        self.last_snapshot = snapshot

        logger.debug(
            f"Market snapshot updated | "
            f"{self.symbol} | "
            f"price={price} | "
            f"mark={mark_price} | "
            f"funding={funding_rate} | "
            f"OI={open_interest}"
        )

        return snapshot
        
# ==========================================================
# MARKET STATUS / HELPERS
# ==========================================================

    async def refresh(self) -> MarketSnapshot:
        """
        Обновляет и возвращает текущий MarketSnapshot.

        Это основной метод, который впоследствии будет
        вызываться торговым циклом.
        """

        return await self.get_market_snapshot()


    # ======================================================

    def get_last_snapshot(
        self
    ) -> Optional[MarketSnapshot]:
        """
        Возвращает последний полученный снимок рынка.

        Если snapshot ещё ни разу не получался,
        возвращает None.
        """

        return self.last_snapshot


    # ======================================================

    def get_last_price(self) -> float:
        """
        Возвращает последнюю известную цену.

        Важно:
        это локально сохранённое значение, а не новый
        запрос к BingX.
        """

        return self.last_price


    # ======================================================

    async def ping(self) -> bool:
        """
        Проверяет доступность API BingX.

        Используется для диагностики соединения.
        """

        try:

            await self.get_price()

            return True

        except Exception as exc:

            logger.warning(
                f"BingX API ping failed: {exc}"
            )

            return False


    # ======================================================

    async def wait_for_connection(
        self,
        attempts: int = 5,
        delay: float = 2.0
    ) -> bool:
        """
        Пытается дождаться доступности BingX API.

        Полезно при запуске VPS, когда сеть может
        подняться не мгновенно.
        """

        for attempt in range(1, attempts + 1):

            if await self.ping():

                logger.success(
                    "BingX API connection established"
                )

                return True

            logger.warning(
                f"BingX connection attempt "
                f"{attempt}/{attempts} failed"
            )

            if attempt < attempts:

                await asyncio.sleep(delay)

        logger.error(
            "Unable to connect to BingX API"
        )

        return False


# ==========================================================
# CONTEXT MANAGER
# ==========================================================

    async def __aenter__(self):
        """
        Позволяет использовать:

            async with BingXMarket() as market:
                ...
        """

        await self.connect()

        return self


    async def __aexit__(
        self,
        exc_type,
        exc_value,
        traceback
    ):
        """
        Автоматически закрывает HTTP-сессию.
        """

        await self.close()


# ==========================================================
# TEST / DEBUG
# ==========================================================

async def test_market():
    """
    Локальный тест market.py.

    Этот тест НЕ открывает никаких сделок.

    Он только проверяет получение рыночных данных.
    """

    logger.info(
        "Starting market.py diagnostic test..."
    )

    async with BingXMarket() as market:

        # --------------------------------------------------
        # Проверяем API
        # --------------------------------------------------

        connected = await market.wait_for_connection()

        if not connected:

            logger.error(
                "Market test failed: "
                "BingX API unavailable"
            )

            return

        # --------------------------------------------------
        # Получаем цену
        # --------------------------------------------------

        price = await market.get_price()

        logger.info(
            f"Current price: {price}"
        )

        # --------------------------------------------------
        # Mark Price
        # --------------------------------------------------

        mark_price = await market.get_mark_price()

        logger.info(
            f"Mark price: {mark_price}"
        )

        # --------------------------------------------------
        # Funding
        # --------------------------------------------------

        funding = await market.get_funding_rate()

        logger.info(
            f"Funding rate: {funding}"
        )

        # --------------------------------------------------
        # Open Interest
        # --------------------------------------------------

        open_interest = await market.get_open_interest()

        logger.info(
            f"Open interest: {open_interest}"
        )

        # --------------------------------------------------
        # Orderbook
        # --------------------------------------------------

        (
            bid,
            ask,
            bid_volume,
            ask_volume
        ) = await market.get_best_bid_ask()

        logger.info(
            f"Best bid: {bid}"
        )

        logger.info(
            f"Best ask: {ask}"
        )

        logger.info(
            f"Bid volume: {bid_volume}"
        )

        logger.info(
            f"Ask volume: {ask_volume}"
        )

        # --------------------------------------------------
        # Spread
        # --------------------------------------------------

        spread = ask - bid

        logger.info(
            f"Spread: {spread}"
        )

        if bid > 0:

            spread_percent = (
                spread / bid
            ) * 100

            logger.info(
                f"Spread %: "
                f"{spread_percent:.6f}%"
            )

        # --------------------------------------------------
        # Candles
        # --------------------------------------------------

        candles_1m = await market.get_1m_candles(
            limit=10
        )

        candles_5m = await market.get_5m_candles(
            limit=10
        )

        candles_15m = await market.get_15m_candles(
            limit=10
        )

        logger.info(
            f"1m candles received: "
            f"{len(candles_1m)}"
        )

        logger.info(
            f"5m candles received: "
            f"{len(candles_5m)}"
        )

        logger.info(
            f"15m candles received: "
            f"{len(candles_15m)}"
        )

        # --------------------------------------------------
        # Full snapshot
        # --------------------------------------------------

        snapshot = await market.get_market_snapshot(
            candle_limit=50
        )

        logger.success(
            "Market snapshot successfully received"
        )

        logger.info(
            f"Symbol: {snapshot.symbol}"
        )

        logger.info(
            f"Price: {snapshot.price}"
        )

        logger.info(
            f"Mark price: {snapshot.mark_price}"
        )

        logger.info(
            f"Funding: {snapshot.funding_rate}"
        )

        logger.info(
            f"Open interest: "
            f"{snapshot.open_interest}"
        )

        logger.info(
            f"1m candles: "
            f"{len(snapshot.candles_1m)}"
        )

        logger.info(
            f"5m candles: "
            f"{len(snapshot.candles_5m)}"
        )

        logger.info(
            f"15m candles: "
            f"{len(snapshot.candles_15m)}"
        )

        # --------------------------------------------------
        # Последняя свеча
        # --------------------------------------------------

        if snapshot.candles_1m:

            candle = snapshot.candles_1m[-1]

            logger.info(
                "Latest 1m candle:"
            )

            logger.info(
                f"  Open: {candle['open']}"
            )

            logger.info(
                f"  High: {candle['high']}"
            )

            logger.info(
                f"  Low: {candle['low']}"
            )

            logger.info(
                f"  Close: {candle['close']}"
            )

            logger.info(
                f"  Volume: {candle['volume']}"
            )

    logger.success(
        "Market test completed"
    )


# ==========================================================
# DIRECT EXECUTION
# ==========================================================

if __name__ == "__main__":

    asyncio.run(
        test_market()
    )