from atproto import Client
from atproto import models

client = Client()
client.login('projectbdbluesky@proton.me', '36YGK7^z&48zen')




"""INITIAL ID"""

client_did = client.app.bsky.actor.get_profile(
    params={'actor': "projectdbbluesky.bsky.social"}
).did

print(client_did)



"""POST"""
# params = models.AppBskyFeedSearchPosts.Params(
#     q="politics",
#     limit=100
# )

# res = client.app.bsky.feed.search_posts(params=params)
#
# print(res)

# for post_view in res.posts:
#     text = post_view.record.text
#     author = post_view.author.handle
#     created_at = post_view.record.created_at
#     print(author, ":", text)

"""LIKED POST"""

res = client.app.bsky.feed.get_actor_likes(
    params={'actor': client_did, 'limit': 50}
)

print(res)


for item in res.feed:
    post = item.post  # app.bsky.feed.defs#postView

    # Sécurité : certains champs peuvent manquer
    record = getattr(post, "record", None)
    if record is None or not hasattr(record, "text"):
        continue

    data = {
        "post_uri": post.uri,
        "post_cid": post.cid,
        "author_did": post.author.did,
        "author_handle": post.author.handle,
        "text": record.text,
        "created_at": getattr(record, "created_at", None),
    }

    print(data)