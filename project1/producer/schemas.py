from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional


class CryptoTick(BaseModel):
    symbol:    str
    price:     float
    volume:    float
    timestamp: datetime
    exchange:  str
    bid:       Optional[float] = None
    ask:       Optional[float] = None

    @field_validator("price", "volume")
    @classmethod
    def must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Value must be positive, got {v}")
        return round(v, 8)

    @field_validator("symbol")
    @classmethod
    def symbol_format(cls, v: str) -> str:
        v = v.upper().strip()
        if "/" not in v:
            raise ValueError(f"Symbol must be in format BTC/USDT, got {v}")
        return v

    def to_dict(self) -> dict:
        return {
            "symbol":    self.symbol,
            "price":     self.price,
            "volume":    self.volume,
            "timestamp": self.timestamp.isoformat(),
            "exchange":  self.exchange,
            "bid":       self.bid,
            "ask":       self.ask,
        }