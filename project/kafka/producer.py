from confluent_kafka import Producer
from atproto import Client
from atproto import models
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})

def send_to_kafka(topic, data):
    producer.produce(topic, json.dumps(data).encode("utf-8"))
    producer.flush()



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
print("Sent SEED user to Kafka!")


"""HELPERS"""

def get_likers(uri, limit=5):
    res = client.app.bsky.feed.get_likes(
        params={
            "uri": uri,
            "limit": limit
        }
    )

    users = []
    for like in res.likes:
        actor = like.actor

        users.append({
            "did": actor.did,
            "handle": actor.handle,
            "display_name": actor.display_name,
        })

    return users

"""LIKED POST"""

res = client.app.bsky.feed.get_actor_likes(
    params={'actor': "did:plc:izxrgd74aaftvtqk7tg4cs3t", 'limit': 10}
)


# print(res)

posts = []
for item in res.feed:
    post = item.post  # app.bsky.feed.defs#postView

    # Sécurité : certains champs peuvent manquer
    record = getattr(post, "record", None)
    if record is None or not hasattr(record, "text"):
        continue
    posts.append({
        "uri": post.cid,
        "cid": post.uri,
        # "author_did": post.author.did,
        # "author_handle": post.author.handle,
        "text": record.text,
        # "created_at": getattr(record, "created_at", None),
        "liker" : profile.did
    })
print(posts)
for p in posts:
    producer.produce("posts", json.dumps(p).encode("utf-8"))
    likers = get_likers(p["cid"], limit=5)
    print("likers", likers)
    for liker in likers:
        # liker["liked_uri"] = p["uri"]
        producer.produce("user", json.dumps(liker).encode("utf-8"))
        print("Sent users link to this post!")
producer.flush()
print("Sent posts to Kafka!")




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
