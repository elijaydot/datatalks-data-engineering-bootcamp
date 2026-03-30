import json
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def parse_tick(raw_message: str) -> dict | None:
    """Parse raw JSON string from Redpanda into a dict."""
    try:
        tick = json.loads(raw_message)
        required = ["symbol", "price", "volume", "timestamp", "exchange"]
        if not all(k in tick for k in required):
            logger.warning(f"Missing fields in tick: {tick}")
            return None
        return tick
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message: {e}")
        return None


def is_stale(tick: dict, max_age_seconds: int = 30) -> bool:
    """Return True if tick timestamp is older than max_age_seconds."""
    try:
        ts = datetime.fromisoformat(
            tick["timestamp"].replace("Z", "+00:00")
        )
        age = datetime.now(timezone.utc) - ts
        return age > timedelta(seconds=max_age_seconds)
    except Exception:
        return True


def normalize_tick(tick: dict) -> dict:
    """Clean and normalize a tick before writing to Cassandra."""
    price = round(float(tick["price"]), 2)
    volume = round(float(tick["volume"]), 8)

    # Fill missing bid/ask with price estimate
    bid = tick.get("bid") or round(price * 0.9999, 2)
    ask = tick.get("ask") or round(price * 1.0001, 2)

    return {
        "symbol":    tick["symbol"],
        "timestamp": tick["timestamp"],
        "price":     price,
        "volume":    volume,
        "exchange":  tick["exchange"],
        "bid":       bid,
        "ask":       ask,
    }


def detect_anomaly(tick: dict, prev_tick: dict | None) -> dict | None:
    """
    Compare current tick to previous tick for the same symbol.
    Returns an anomaly dict if detected, otherwise None.
    """
    if prev_tick is None:
        return None

    price_change_pct = abs(
        (tick["price"] - prev_tick["price"]) / prev_tick["price"] * 100
    )
    volume_change_pct = abs(
        (tick["volume"] - prev_tick["volume"]) / (prev_tick["volume"] + 1e-9) * 100
    )

    if price_change_pct > 2.0:
        return {
            "symbol":         tick["symbol"],
            "timestamp":      tick["timestamp"],
            "price":          tick["price"],
            "volume":         tick["volume"],
            "anomaly_type":   "PRICE_JUMP",
            "anomaly_detail": f"Price changed {price_change_pct:.2f}% in one tick",
        }

    if volume_change_pct > 500:
        return {
            "symbol":         tick["symbol"],
            "timestamp":      tick["timestamp"],
            "price":          tick["price"],
            "volume":         tick["volume"],
            "anomaly_type":   "VOLUME_SPIKE",
            "anomaly_detail": f"Volume changed {volume_change_pct:.2f}% in one tick",
        }

    return None