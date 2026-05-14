from kafka import KafkaProducer
import json, time, random
from datetime import datetime, timedelta, timezone

# Windows에서 Docker Kafka에 접속하므로 localhost:19092 사용
BOOTSTRAP = "localhost:19092"

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=50
)

def now_iso(offset_seconds=0):
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).isoformat()

event_types = ["view", "click", "purchase"]
users = [f"user_{i}" for i in range(1, 6)]
products = [f"product_{i}" for i in range(1, 4)]

i = 0
print("Kafka multi-topic producer started. Ctrl+C로 종료하세요.", flush=True)

while True:
    late_offset = random.choice([0, 0, 0, -30, -60, -120])
    user_id = random.choice(users)
    product_id = random.choice(products)
    event_type = random.choice(event_types)

    event = {
        "event_id": f"evt_{i}",
        "id": i,
        "user_id": user_id,
        "product_id": product_id,
        "event_type": event_type,
        "message": f"{event_type} event {i}",
        "event_time": now_iso(late_offset),
        "processing_hint_time": now_iso(0),
        "timestamp": time.time(),
        "value": round(random.uniform(1, 100), 2),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(1000, 10000), 2)
    }

    producer.send("test-topic", key=user_id, value=event)
    producer.send("events", key=user_id, value=event)

    impression = {
        "impression_id": f"imp_{i}",
        "user_id": user_id,
        "ad_id": product_id,
        "impression_time": event["event_time"],
        "campaign": random.choice(["A", "B"])
    }
    producer.send("impressions", key=user_id, value=impression)

    click = None
    if random.random() < 0.7:
        click_delay = random.randint(0, 90)
        click = {
            "click_id": f"clk_{i}",
            "user_id": user_id,
            "ad_id": product_id,
            "click_time": now_iso(late_offset + click_delay),
            "device": random.choice(["mobile", "web"])
        }
        producer.send("clicks", key=user_id, value=click)

    if i % 10 == 0:
        producer.flush()
        print(f"sent #{i}: {event}", flush=True)
        print("impression:", impression, flush=True)
        if click is not None:
            print("click:", click, flush=True)
        print("-" * 60, flush=True)

    i += 1
    time.sleep(1)