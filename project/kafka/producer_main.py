from confluent_kafka import Producer
from atproto import Client
import json
import time
import os

############################################################
# 1. CONFIGURATION
############################################################
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_USERS = os.getenv("KAFKA_TOPIC_USERS")
TOPIC_POSTS = os.getenv("KAFKA_TOPIC_POSTS")

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")

MAX_LIKERS = int(os.getenv("MAX_LIKERS", 20))
MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", 2))
SEED_HANDLE = os.getenv("SEED_HANDLE")

############################################################
# 2. CLIENTS
############################################################
print(KAFKA_BROKER)
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

client = Client()
client.login(BLUESKY_HANDLE, BLUESKY_PASSWORD)

############################################################
# 2. KAFKA SENDER
############################################################

def send_to_kafka(topic, payload):
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()

############################################################
# 3. HELPERS : BLUESKY API WRAPPERS
############################################################

def get_user_posts(did, limit=10):
    res = client.app.bsky.feed.get_author_feed(
        params={"actor": did, "limit": limit}
    )

    posts = []
    for item in res.feed:
        post = item.post
        record = getattr(post, "record", None)
        if record and hasattr(record, "text"):
            curr = {
                "uri": post.uri,
                "cid": post.cid,
                "text": record.text,
                "author": did,
            }
            posts.append(curr)
            # print("AUTHOR", curr["author"])
    return posts

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

def crawl(seed_handle):
    seed_profile = get_profile(seed_handle)
    seed_did = seed_profile["did"]

    seen_users = set([seed_did])
    seen_posts = set()

    # --- STEP 1 : posts likés par le seed ---
    first_wave_users = set()

    liked = get_actor_likes(seed_did, limit=10)
    for post in liked:
        seen_posts.add(post["uri"])
        likers = get_likers(post["uri"], MAX_LIKERS)
        for user in likers:
            if user["did"] not in seen_users:
                first_wave_users.add(user["did"])

    current_users = first_wave_users

    # --- STEP 2 : propagation sur 5 itérations ---
    for depth in range(MAX_ITERATIONS):
        print(f"\n=== ITERATION {depth + 1} ===")
        next_users = set()

        for did in current_users:
            if did in seen_users:
                continue

            seen_users.add(did)
            print("user send", did)
            # send_to_kafka("users", did)
            send_to_kafka(TOPIC_USERS, get_profile(did))

            posts = get_user_posts(did, limit=10)
            for post in posts:
                if post["uri"] in seen_posts:
                    continue

                seen_posts.add(post["uri"])
                print("post send", post["uri"])
                print("post send author", post["author"])
                send_to_kafka(TOPIC_POSTS, post)

                likers = get_likers(post["uri"], MAX_LIKERS)
                for liker in likers:
                    if liker["did"] not in seen_users:
                        liked = {
                            "user_did": liker["did"],
                            "uri": post["uri"],
                            "type": "LIKED"
                        }
                        send_to_kafka(TOPIC_USERS, liked)
                        next_users.add(liker["did"])

        current_users = next_users
        print("users seen : ", seen_users)
        print("posts seen : ", seen_posts)

    print("Crawl finished")

############################################################
# 5. SEED START
############################################################

if __name__ == "__main__":
    seed = SEED_HANDLE  # handle
    seed_profile = get_profile(seed)
    crawl(seed_profile["did"])

    print("\nCrawling finished!")
