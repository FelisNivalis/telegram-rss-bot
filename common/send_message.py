# coding: utf-8

import json
import requests
import datetime
import time
from collections import defaultdict, Counter
from loguru import logger
from const import MESSAGE_FORMAT, MESSAGE_TYPE, FUNCS
from common.formatter import EscapeFstringFormatter


last_time_send_message = datetime.datetime(1, 1, 1)
last_time_send_message_by_chat = defaultdict(lambda: datetime.datetime(1, 1, 1))
last_num_message = 1
last_num_message_by_chat = defaultdict(lambda: 1)


def sleep_until(until: datetime.datetime):
    while (now := datetime.datetime.now()) < until:
        time.sleep((until - now).total_seconds())
    return datetime.datetime.now()


def _send_message(bot_token: str, chat_id: str, message_type: str=MESSAGE_TYPE, **kwargs):
    # https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
    global last_time_send_message, last_time_send_message_by_chat, last_num_message, last_num_message_by_chat
    last_time_send_message = sleep_until(last_time_send_message + datetime.timedelta(seconds=0.05 * last_num_message))
    last_time_send_message_by_chat[chat_id] = sleep_until(last_time_send_message_by_chat[chat_id] + datetime.timedelta(seconds=3 * last_num_message_by_chat[chat_id]))
    if message_type == "MediaGroup":
        last_num_message = max(1, len(json.loads(kwargs.get("media", "[]"))))
    else:
        last_num_message = 1
    last_num_message_by_chat[chat_id] = last_num_message
    return requests.get(f"https://api.telegram.org/bot{bot_token}/send{message_type}", params={"chat_id": chat_id} | kwargs)


def send_message(bot_token: str, chat_id: str, item, config, report=None):
    message_config = config.get("message_config", {})
    message_type = message_config.get("type", MESSAGE_TYPE)
    message_args = ({"text": MESSAGE_FORMAT} if message_type == MESSAGE_TYPE else {}) | message_config.get("args", {})
    parse_mode = message_args.get("parse_mode", "")

    ret = _send_message(
        bot_token, chat_id, message_type,
        **{
            k: EscapeFstringFormatter(
                parse_mode
                if k in ["text", "caption"]
                else "", FUNCS
            ).format(v, **item)
            for k, v in message_args.items()
        }
    )

    if report is not None and "send_message_errors" not in report:
        report["send_message_errors"] = Counter()
    if not (ret_json := json.loads(ret.text))["ok"]:
        logger.error(f"Send {message_type} to chat `{chat_id}` failed.")
        logger.debug(f"{message_args=}")
        logger.debug(f"url={ret.url}")
        logger.debug(f"response={ret.text}")
        if ret_json.get("error_code") == 429 and isinstance((retry_after := ret_json.get("parameters", {}).get("retry_after")), int):
            time.sleep(retry_after)
            return send_message(bot_token, chat_id, item, config)
        if report is not None:
            report["send_message_errors"][chat_id] += 1
