# coding: utf-8

import os
import redis

INTERVAL = 1800
ITEM_XPATH = "/rss/channel/item"
FIELDS_XPATH = {
    "link": "./link",
    "title": "./title",
    "description": "./description",
    "pubDate": "./pubDate",
}
MESSAGE_FORMAT = "{title}\n{description}\n{pubDate}\n{link}"
GROUP_CONFIG_FIELDS = {
    "message_format",
    "parse_mode",
}
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
