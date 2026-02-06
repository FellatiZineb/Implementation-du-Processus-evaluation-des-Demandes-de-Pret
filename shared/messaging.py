# shared/messaging.py

from __future__ import annotations

import json
import logging
import time
from typing import Callable, Optional

import pika

from shared.constants import EXCHANGE_NAME, EXCHANGE_TYPE

logger = logging.getLogger("messaging")


def _connect(amqp_url: str, retries: int = 30, delay_s: float = 1.0) -> pika.BlockingConnection:
    last_err: Optional[Exception] = None
    for _ in range(retries):
        try:
            params = pika.URLParameters(amqp_url)
            return pika.BlockingConnection(params)
        except Exception as e:
            last_err = e
            logger.warning("RabbitMQ not ready, retrying: %s", e)
            time.sleep(delay_s)
    raise RuntimeError(f"Failed to connect to RabbitMQ after {retries} retries: {last_err}")


def setup_exchange(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)


def publish_event(amqp_url: str, routing_key: str, event_dict: dict) -> None:
    conn = _connect(amqp_url)
    try:
        ch = conn.channel()
        setup_exchange(ch)
        body = json.dumps(event_dict).encode("utf-8")

        ch.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
            ),
        )
        logger.info("Published event routing_key=%s", routing_key)
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
        except Exception as e:
            logger.exception("Error processing message, will nack and requeue: %s", e)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=queue_name, on_message_callback=_callback)
    logger.info("Consuming queue=%s bindings=%s", queue_name, binding_keys)
    ch.start_consuming()
