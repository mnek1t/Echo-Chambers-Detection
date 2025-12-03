from confluent_kafka import Producer
from atproto import Client
from atproto import models
import json

p = Producer({"bootstrap.servers": "localhost:9092"})

def send_to_kafka(topic, data):
    p.produce(topic, json.dumps(data).encode("utf-8"))
    p.flush()



client = Client()
client.login('projectbdbluesky@proton.me', '36YGK7^z&48zen')

profile = client.app.bsky.actor.get_profile(
    params={'actor': "projectdbbluesky.bsky.social"}
)

data = {
    "did": profile.did,
    "handle": profile.handle,
    "display_name": profile.display_name,
    "description": profile.description,
}

send_to_kafka("user", data)
print("Sent to Kafka!")




"""OLD"""
# producer = Producer({'bootstrap.servers': 'localhost:9092'})
#
# def delivery_report(err, msg):
#     if err:
#         print("Delivery failed:", err)
#     else:
#         print(f"Message sent to {msg.topic()} [{msg.partition()}]")
#
# text_post = "I want to eat healthier and reduce sugar"
#
# producer.produce(
#     topic="new_post",
#     value=text_post.encode("utf-8"),
#     callback=delivery_report
# )
#
# producer.flush()
