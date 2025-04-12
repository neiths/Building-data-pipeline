from kafka import KafkaProducer
import json
import time
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'event_tracking'

def generate_event():
    return {
        "event_time": fake.iso8601(),
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(["click", "view", "purchase"]),
        "page": fake.uri_path(),
        "ip": fake.ipv4()
    }

print(f"Producing to topic: {topic}")
while True:
    event = generate_event()
    print(f"Sending: {event}")
    producer.send(topic, event)
    time.sleep(1)
