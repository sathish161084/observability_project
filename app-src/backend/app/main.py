import asyncio
import json
import logging
import os
import random
import ssl
import time
from contextlib import asynccontextmanager
from typing import Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pythonjsonlogger import jsonlogger

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger("backend")

REQUEST_COUNT = Counter(
    "app_http_requests_total",
    "Total HTTP requests",
    ["endpoint", "method", "status"],
)

REQUEST_LATENCY = Histogram(
    "app_http_request_duration_seconds",
    "HTTP request latency",
    ["endpoint", "method"],
)

tracer = trace.get_tracer("backend.api")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_RETRY_ATTEMPTS = int(os.getenv("KAFKA_RETRY_ATTEMPTS", "20"))
KAFKA_RETRY_DELAY_SECONDS = int(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "10"))
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")

producer: Optional[AIOKafkaProducer] = None


def configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    root.handlers.clear()

    handler = logging.StreamHandler()
    handler.setFormatter(
        jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    root.addHandler(handler)

    LoggingInstrumentor().instrument(set_logging_format=False)


def configure_tracing() -> None:
    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://otel-collector.observability.svc.cluster.local:4317",
    )

    resource = Resource.create(
        {
            SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "backend"),
            "deployment.environment": os.getenv("DEPLOYMENT_ENV", "dev"),
            "service.namespace": "app",
        }
    )

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=endpoint,
                insecure=True,
            )
        )
    )
    trace.set_tracer_provider(provider)


def build_ssl_context() -> Optional[ssl.SSLContext]:
    if KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
        return None
    return ssl.create_default_context()


async def create_kafka_producer() -> AIOKafkaProducer:
    kwargs = {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "security_protocol": KAFKA_SECURITY_PROTOCOL,
    }

    ssl_context = build_ssl_context()
    if ssl_context is not None:
        kwargs["ssl_context"] = ssl_context

    kafka_producer = AIOKafkaProducer(**kwargs)
    await kafka_producer.start()
    return kafka_producer


async def connect_kafka_background() -> None:
    global producer

    if not KAFKA_BOOTSTRAP:
        logger.warning("KAFKA_BOOTSTRAP is empty; backend will run without Kafka")
        return

    for attempt in range(1, KAFKA_RETRY_ATTEMPTS + 1):
        try:
            producer = await create_kafka_producer()
            logger.info("Kafka producer connected")
            return
        except Exception as exc:
            logger.warning(
                "Kafka connection failed on attempt %s/%s: %s",
                attempt,
                KAFKA_RETRY_ATTEMPTS,
                exc,
            )
            await asyncio.sleep(KAFKA_RETRY_DELAY_SECONDS)

    logger.error("Kafka producer could not connect after retries; app will stay up")


configure_logging()
configure_tracing()

app = FastAPI(title="three-tier-backend", version="1.0.0")
FastAPIInstrumentor.instrument_app(app)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(connect_kafka_background())
    try:
        yield
    finally:
        task.cancel()
        if producer is not None:
            await producer.stop()
            logger.info("Kafka producer stopped")


app.router.lifespan_context = lifespan


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    status = "500"

    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    finally:
        REQUEST_COUNT.labels(
            endpoint=request.url.path,
            method=request.method,
            status=status,
        ).inc()
        REQUEST_LATENCY.labels(
            endpoint=request.url.path,
            method=request.method,
        ).observe(time.perf_counter() - start)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "kafka_connected": producer is not None,
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/products")
async def products():
    with tracer.start_as_current_span("list_products"):
        await asyncio.sleep(random.uniform(0.02, 0.08))
        logger.info("listing products")
        return [
            {"id": "sku-100", "name": "Laptop", "price": 1299},
            {"id": "sku-101", "name": "Headset", "price": 99},
            {"id": "sku-102", "name": "Dock", "price": 189},
        ]


@app.get("/api/slow")
async def slow():
    with tracer.start_as_current_span("slow_request"):
        await asyncio.sleep(random.uniform(0.6, 1.2))
        logger.warning("slow request generated")
        return {"status": "slow-response"}


@app.post("/api/checkout")
async def checkout(payload: dict):
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    order_id = f"ord-{random.randint(100000, 999999)}"

    with tracer.start_as_current_span("checkout_request"):
        if payload.get("force_error"):
            logger.error("forced checkout error")
            raise HTTPException(status_code=500, detail="forced checkout error")

        event = {
            "order_id": order_id,
            "customer_id": payload.get("customer_id", "anonymous"),
            "items": payload.get("items", []),
            "amount": payload.get("amount", 0),
            "simulate_failure": payload.get("simulate_failure", False),
        }

        await producer.send_and_wait(ORDER_TOPIC, json.dumps(event).encode("utf-8"))
        logger.info("order published", extra={"order_id": order_id})

        return {
            "status": "accepted",
            "order_id": order_id,
        }