# coding: utf-8

import os
import redis

INTERVAL = 60
ITEM_XPATH = "/rss/channel/item"
FIELDS_XPATH = {
    "link": "./link",
    "title": "./title",
    "description": "./description",
    "pubDate": "./pubDate",
}
MESSAGE_TYPE = "Message"
MESSAGE_FORMAT = "{title}\n{description}\n{pubDate}\n{link}"
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

def get_filters():
    exec(r.get("filters") or "{}")
    return locals()
FILTERS = get_filters()
