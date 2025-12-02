from atproto import Client
from atproto import models

client = Client()
client.login('projectbdbluesky@proton.me', '36YGK7^z&48zen')

params = models.AppBskyFeedSearchPosts.Params(
    q="politics",
    limit=100
)

res = client.app.bsky.feed.search_posts(params=params)

print(res)

# for post_view in res.posts:
#     text = post_view.record.text
#     author = post_view.author.handle
#     created_at = post_view.record.created_at
#     print(author, ":", text)
