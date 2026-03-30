import ccxt
import logging
from datetime import datetime, timezone
from schemas import CryptoTick

logger = logging.getLogger(__name__)


class CryptoFetcher:
    def __init__(self, exchange_name: str, symbols: list[str]):
        self.symbols = symbols
        self.exchange = self._init_exchange(exchange_name)

    def _init_exchange(self, name: str):
        exchange_class = getattr(ccxt, name)
        exchange = exchange_class({
            "enableRateLimit": True,   # respect exchange rate limits
            "timeout": 10000,
        })
        logger.info(f"Connected to exchange: {name}")
        return exchange

    def fetch_ticks(self) -> list[CryptoTick]:
        """Fetch latest ticker for all configured symbols."""
        ticks = []
        for symbol in self.symbols:
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                tick = CryptoTick(
                    symbol=symbol,
                    price=ticker["last"],
                    volume=ticker["baseVolume"] or 0.0,
                    timestamp=datetime.fromtimestamp(
                        ticker["timestamp"] / 1000,
                        tz=timezone.utc
                    ),
                    exchange=self.exchange.id,
                    bid=ticker.get("bid"),
                    ask=ticker.get("ask"),
                )
                ticks.append(tick)
                logger.debug(f"Fetched {symbol}: ${tick.price}")
            except Exception as e:
                logger.error(f"Failed to fetch {symbol}: {e}")
        return ticks