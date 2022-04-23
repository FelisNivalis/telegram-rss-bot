# coding: utf-8

import json
from const import r


def get_chat_info(chat_id: str):
    chat_info = json.loads(r.hget("chats", chat_id) or "{}")
    return chat_info
