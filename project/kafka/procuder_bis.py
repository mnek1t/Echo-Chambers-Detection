from confluent_kafka import Producer
from atproto import Client
import json
import time

from joblib.externals.loky.process_executor import MAX_DEPTH

############################################################
# 1. CONFIGURATION
############################################################

producer = Producer({"bootstrap.servers": "localhost:9092"})

client = Client()
client.login('projectbdbluesky@proton.me', '36YGK7^z&48zen')

# Pour éviter les doublons :
SEEN_USERS = set()
SEEN_POSTS = set()

MAX_USERS = 50

############################################################
# 2. KAFKA SENDER
############################################################

def send_to_kafka(topic, payload):
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()

############################################################
# 3. HELPERS : BLUESKY API WRAPPERS
############################################################

def get_profile(did_or_handle):
    profile = client.app.bsky.actor.get_profile(
        params={"actor": did_or_handle}
    )
    return {
        "did": profile.did,
        "handle": profile.handle,
        "display_name": profile.display_name,
        "description": getattr(profile, "description", ""),
    }

def safe_get_actor_likes(did, limit=10):
    try:
        return client.app.bsky.feed.get_actor_likes(
            params={"actor": did, "limit": limit}
        )
    except Exception as e:
        print(f"[WARN] Cannot fetch likes for {did}: {e}")
        return None

def get_actor_likes(did, limit=10):
    """Retourne les posts likés par un utilisateur."""
    res = client.app.bsky.feed.get_actor_likes(
        params={"actor": did, "limit": limit}
    )

    posts = []
    for item in res.feed:
        post = item.post
        record = getattr(post, "record", None)
        if not record or not hasattr(record, "text"):
            continue

        posts.append({
            "uri": post.uri,   # AT-URI (correct)
            "cid": post.cid,
            "text": record.text,
            "liked_by": did,
        })
    return posts


def get_likers(uri, limit=5):
    """Retourne les utilisateurs ayant liké un post."""
    res = client.app.bsky.feed.get_likes(
        params={"uri": uri, "limit": limit}
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

############################################################
# 4. MAIN CRAWLING LOGIC
############################################################

def process_user(did,depth=0):
    if depth > MAX_DEPTH:
        return
    """Traite un utilisateur : envoie Kafka + collecte posts likés."""
    if did in SEEN_USERS:
        return
    SEEN_USERS.add(did)

    print(f"\n=== Processing USER: {did} ===")

    # 1. récupérer profil → kafka
    profile_data = get_profile(did)
    send_to_kafka("user", profile_data)

    # 2. récupérer posts likés → kafka
    liked_posts = safe_get_actor_likes(did)
    for post in liked_posts:
        process_post(post, depth + 1)


def process_post(post, depth=0):
    """Traite un post et envoie Kafka + découvre ses likers."""
    if post["uri"] in SEEN_POSTS:
        return
    SEEN_POSTS.add(post["uri"])

    print(f"  -> Processing POST: {post['uri']}")

    send_to_kafka("posts", post)

    # 1. récupérer les likers du post
    print(post)
    print(post["cid"])
    likers = get_likers(post["uri"], limit=5)

    for liker in likers:
        send_to_kafka("user", liker)
        process_user(liker["did"], depth)

############################################################
# 5. SEED START
############################################################

if __name__ == "__main__":
    seed = "projectdbbluesky.bsky.social"  # handle
    seed_profile = get_profile(seed)
    process_user(seed_profile["did"])

    print("\nCrawling finished!")



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
