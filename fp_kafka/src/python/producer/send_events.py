# from kafka import KafkaProducer
# import json
# import time
#
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )
#
# while True:
#     event = {
#         "user_id": "u1",
#         "action": "click",
#         "timestamp": int(time.time())
#     }
#     producer.send("user-events", event)
#     print("Sent:", event)
#     time.sleep(1)
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()
producer = KafkaProducer(bootstrap_servers='localhost:9092')

actions = ["click", "view", "purchase", "signup"]

while True:
    event = {
        "user_id": fake.uuid4(),
        "action": random.choice(actions),
        "timestamp": int(time.time() * 1000)
    }

    producer.send("user-events", json.dumps(event).encode('utf-8'))
    print(f"Sent: {event}")
    time.sleep(1)  # adjustable speed
