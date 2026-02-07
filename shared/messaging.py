# shared/messaging.py

from __future__ import annotations

import json
import logging
import time
from typing import Callable, Optional
from urllib.parse import urlparse, unquote

import pika
from pydantic import ValidationError

from shared.constants import EXCHANGE_NAME, EXCHANGE_TYPE

logger = logging.getLogger("messaging")


def _amqp_context(amqp_url: str) -> str:
    try:
        u = urlparse(amqp_url)
        host = u.hostname or ""
        port = u.port or ""
        vhost = unquote((u.path or "/")[1:]) if (u.path or "/") != "/" else "/"
        return f"host={host} port={port} vhost={vhost}"
    except Exception:
        return "host=? port=? vhost=?"


def _connect(amqp_url: str, retries: int = 30, delay_s: float = 1.0) -> pika.BlockingConnection:
    if not amqp_url:
        raise RuntimeError("AMQP_URL is not set")

    last_err: Optional[Exception] = None
    ctx = _amqp_context(amqp_url)

    for attempt in range(1, retries + 1):
        try:
            params = pika.URLParameters(amqp_url)
            conn = pika.BlockingConnection(params)
            logger.info("RabbitMQ connected | %s", ctx)
            return conn
        except Exception as e:
            last_err = e
            logger.warning("RabbitMQ not ready, retrying | attempt=%d/%d | %s | err=%s", attempt, retries, ctx, e)
            time.sleep(delay_s)

    raise RuntimeError(f"Failed to connect to RabbitMQ after {retries} retries: {last_err}")


def setup_exchange(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)


def publish_event(amqp_url: str, routing_key: str, event_dict: dict) -> None:
    conn = _connect(amqp_url)
    try:
        ch = conn.channel()
        setup_exchange(ch)

        # Publisher confirms: lets you detect unroutable or failed publishes.
        ch.confirm_delivery()

        body = json.dumps(event_dict, default=str).encode("utf-8")

        ok = ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
            mandatory=False,
        )

        logger.info(
            "Published event | exchange=%s | routing_key=%s | bytes=%d | confirmed=%s | %s",
            EXCHANGE_NAME,
            routing_key,
            len(body),
            ok,
            _amqp_context(amqp_url),
        )
    finally:
        conn.close()


def consume_events(
    amqp_url: str,
    queue_name: str,
    binding_keys: list[str],
    on_message: Callable[[dict], None],
) -> None:
    conn = _connect(amqp_url)
    ch = conn.channel()
    setup_exchange(ch)

    ch.queue_declare(queue=queue_name, durable=True)
    for key in binding_keys:
        ch.queue_bind(queue=queue_name, exchange=EXCHANGE_NAME, routing_key=key)

    def _callback(channel, method, properties, body: bytes):
        try:
            payload = json.loads(body.decode("utf-8"))
            on_message(payload)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValidationError as e:
            logger.error("Invalid message schema, dropping (no requeue): %s", e)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON, dropping (no requeue): %s", e)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as e:
            logger.exception("Error processing message, requeueing: %s", e)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=queue_name, on_message_callback=_callback)

    logger.info(
        "Consuming | exchange=%s | queue=%s | bindings=%s | %s",
        EXCHANGE_NAME,
        queue_name,
        binding_keys,
        _amqp_context(amqp_url),
    )

    ch.start_consuming()
