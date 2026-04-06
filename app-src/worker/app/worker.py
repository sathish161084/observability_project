import asyncio
import json
import logging
import os
import signal
import ssl

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_RETRY_ATTEMPTS = int(os.getenv("KAFKA_RETRY_ATTEMPTS", "20"))
KAFKA_RETRY_DELAY_SECONDS = int(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "15"))

ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders-dlq")

PROCESSED = Counter("worker_messages_processed_total", "Processed worker messages", ["result"])
running = True


def configure_logging():
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    root.handlers.clear()
    h = logging.StreamHandler()
    h.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(h)


def configure_tracing():
    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector.observability.svc.cluster.local:4317",
    )
    provider = TracerProvider(
        resource=Resource.create(
            {
                SERVICE_NAME: "worker",
                "deployment.environment": os.getenv("DEPLOYMENT_ENV", "dev"),
            }
        )
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)


def build_ssl_context():
    if KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
        return None
    return ssl.create_default_context()


async def connect_kafka_clients():
    ssl_context = build_ssl_context()

    common = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "security_protocol": KAFKA_SECURITY_PROTOCOL,
    }

    if ssl_context:
        common["ssl_context"] = ssl_context

    consumer = AIOKafkaConsumer(
        ORDER_TOPIC,
        group_id="order-worker",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        **common,
    )

    producer = AIOKafkaProducer(**common)

    await consumer.start()
    await producer.start()

    return consumer, producer


async def connect_with_retry():
    logger = logging.getLogger("worker")

    for attempt in range(1, KAFKA_RETRY_ATTEMPTS + 1):
        try:
            consumer, producer = await connect_kafka_clients()
            logger.info("worker kafka clients connected")
            return consumer, producer
        except Exception as exc:
            logger.warning(
                "worker kafka connection failed on attempt %s/%s: %s",
                attempt,
                KAFKA_RETRY_ATTEMPTS,
                exc,
            )
            await asyncio.sleep(KAFKA_RETRY_DELAY_SECONDS)

    raise RuntimeError("worker could not connect to kafka after retries")


async def main():
    configure_logging()
    configure_tracing()

    logger = logging.getLogger("worker")
    tracer = trace.get_tracer("worker")

    start_http_server(9102)

    consumer, producer = await connect_with_retry()

    try:
        while running:
            batches = await consumer.getmany(timeout_ms=1000, max_records=50)
            for _, messages in batches.items():
                for msg in messages:
                    payload = json.loads(msg.value.decode("utf-8"))
                    try:
                        with tracer.start_as_current_span("process_order"):
                            await asyncio.sleep(0.1)
                            if payload.get("simulate_failure"):
                                raise RuntimeError("simulated processing failure")
                            logger.info("processed order")
                            PROCESSED.labels(result="success").inc()
                    except Exception as exc:
                        logger.exception("sending to dlq")
                        PROCESSED.labels(result="failed").inc()
                        await producer.send_and_wait(
                            DLQ_TOPIC,
                            json.dumps({"original": payload, "error": str(exc)}).encode("utf-8"),
                        )
    finally:
        await consumer.stop()
        await producer.stop()


def stop_handler(*_):
    global running
    running = False


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    asyncio.run(main())