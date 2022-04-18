# coding: utf-8

import os
import redis

INTERVAL = 60
ITEM_XPATH = "./channel/item"
FIELDS_XPATH = {
    "link": "./link/text()",
    "title": "./title/text()",
    "description": "./description/text()",
    "pub_date": "./pub_date/text()",
}
MESSAGE_TYPE = "Message"
MESSAGE_FORMAT = "{title}\n{description}\n{pub_date}\n{link}"
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True)

try:
    from my.funcs import FUNCS
except ModuleNotFoundError:
    FUNCS = {}
