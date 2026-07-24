"""Resilient Binance aggregate-trade producer for Kafka."""

import json
import logging
import os
import time
import uuid

import websocket
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-raw-data")
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
SYMBOLS = [symbol.strip().lower() for symbol in os.getenv("CRYPTO_SYMBOLS", "btcusdt,ethusdt").split(",") if symbol.strip()]
RECONNECT_INITIAL_SECONDS = float(os.getenv("WS_RECONNECT_INITIAL_SECONDS", "1"))
RECONNECT_MAX_SECONDS = float(os.getenv("WS_RECONNECT_MAX_SECONDS", "30"))
SOCKET_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{symbol}@aggTrade" for symbol in SYMBOLS)

producer = KafkaProducer(
    bootstrap_servers=[f"{KAFKA_HOST}:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
    retries=5,
)


def on_open(_ws):
    logger.info("Connected to Binance streams: %s", ", ".join(SYMBOLS))


def on_message(_ws, message):
    try:
        envelope = json.loads(message)
        stream_name = envelope.get("stream", "")
        payload = envelope.get("data") or {}
        symbol = payload.get("s")
        if not stream_name or not symbol:
            logger.warning("Ignoring malformed Binance event: %s", message[:300])
            return

        event = {
            "metadata": {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": payload.get("E"),
                "event_type": stream_name.split("@", maxsplit=1)[-1],
                "source": "binance_websocket",
                "schema_version": "1.0",
            },
            "payload": payload,
        }
        producer.send(KAFKA_TOPIC, key=symbol, value=event).get(timeout=10)
        logger.info("Published event_id=%s symbol=%s", event["metadata"]["event_id"], symbol)
    except (json.JSONDecodeError, KeyError, ValueError) as error:
        logger.warning("Ignoring invalid Binance payload: %s", error)
    except Exception:
        logger.exception("Could not publish Binance event to Kafka")


def on_error(_ws, error):
    logger.error("Binance WebSocket error: %s", error)


def on_close(_ws, close_status_code, close_msg):
    logger.warning("Binance connection closed: code=%s message=%s", close_status_code, close_msg)


def run_forever():
    delay = RECONNECT_INITIAL_SECONDS
    while True:
        try:
            ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            logger.exception("Unexpected WebSocket failure")
        logger.info("Reconnecting to Binance in %.1f seconds", delay)
        time.sleep(delay)
        delay = min(delay * 2, RECONNECT_MAX_SECONDS)


if __name__ == "__main__":
    try:
        run_forever()
    finally:
        producer.flush()
        producer.close()
