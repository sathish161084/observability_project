import asyncio
import json
import logging
import os
import random
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
from opentelemetry.sdk.resources import RESOURCE_ATTRIBUTES, SERVICE_NAME, Resource
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
    "HTTP latency",
    ["endpoint", "method"],
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "b-1.example.kafka.amazonaws.com:9092")
ORDER_TOPIC = os.getenv("ORDER_TOPIC", "orders")
DEPLOYMENT_ENV = os.getenv("DEPLOYMENT_ENV", "dev")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4317",
)

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
    resource = Resource.create(
        {
            SERVICE_NAME: "backend",
            "deployment.environment": DEPLOYMENT_ENV,
            "service.namespace": "app",
        }
    )

    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
        insecure=True,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


configure_logging()
configure_tracing()
tracer = trace.get_tracer("backend.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    logger.info("Kafka producer started")

    try:
        yield
    finally:
        if producer is not None:
            await producer.stop()
            logger.info("Kafka producer stopped")


app = FastAPI(lifespan=lifespan)

# Important: instrument after app creation, not inside startup/lifespan body
FastAPIInstrumentor.instrument_app(app)


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
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/products")
async def products():
    with tracer.start_as_current_span("list_products"):
        await asyncio.sleep(random.uniform(0.02, 0.08))
        return [
            {"id": "sku-100", "name": "Laptop", "price": 1299},
            {"id": "sku-101", "name": "Headset", "price": 99},
        ]


@app.get("/api/slow")
async def slow():
    with tracer.start_as_current_span("slow_request"):
        await asyncio.sleep(random.uniform(0.6, 1.3))
        logger.warning("slow request generated")
        return {"status": "slow-response"}


@app.post("/api/checkout")
async def checkout(payload: dict):
    global producer

    order_id = f"ord-{random.randint(100000, 999999)}"

    with tracer.start_as_current_span("checkout_request"):
        if payload.get("force_error"):
            logger.error("forced checkout error")
            raise HTTPException(status_code=500, detail="forced checkout error")

        if producer is None:
            logger.error("Kafka producer is not initialized")
            raise HTTPException(status_code=500, detail="Kafka producer not initialized")

        event = {
            "order_id": order_id,
            "customer_id": payload.get("customer_id", "anonymous"),
            "amount": payload.get("amount", 0),
            "simulate_failure": payload.get("simulate_failure", False),
        }

        await producer.send_and_wait(
            ORDER_TOPIC,
            json.dumps(event).encode("utf-8"),
        )

        logger.info("order published", extra={"order_id": order_id})
        return {"status": "accepted", "order_id": order_id}