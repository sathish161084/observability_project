import asyncio, json, logging, os, signal
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server, Counter
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "b-1.example.kafka.amazonaws.com:9092")
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
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4317")
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "worker", "deployment.environment": os.getenv("DEPLOYMENT_ENV", "dev")}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)

async def main():
    configure_logging()
    configure_tracing()
    logger = logging.getLogger("worker")
    tracer = trace.get_tracer("worker")
    start_http_server(9102)
    consumer = AIOKafkaConsumer(ORDER_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="order-worker", auto_offset_reset="earliest", enable_auto_commit=True)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await consumer.start(); await producer.start()
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
                        await producer.send_and_wait(DLQ_TOPIC, json.dumps({"original": payload, "error": str(exc)}).encode("utf-8"))
    finally:
        await consumer.stop(); await producer.stop()

def stop_handler(*_):
    global running
    running = False

if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    asyncio.run(main())
