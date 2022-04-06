# coding: utf-8

import os
import json
import requests
import dateutil.parser
import datetime
import hashlib
import yaml
import math
import time
import urllib
from lxml import etree
from typing import Dict
from collections import defaultdict
from loguru import logger
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, r
from common.formatter import EscapeFstringFormatter


EXECUTE_TIMESTAMP = datetime.datetime.now().timestamp()
SEND_MESSAGE_INTERVAL = 3


def check_interval(subscription, interval):
    last_fetch_time = r.hget("last_fetch_time", subscription)
    if not last_fetch_time:
        last_fetch_time = 0
    else:
        last_fetch_time = float(last_fetch_time)
    return EXECUTE_TIMESTAMP - last_fetch_time > interval * 60


def update_last_fetch_time(subscriptions):
    if len(subscriptions) == 0:
        return True
    return r.hset('last_fetch_time', mapping={
        subscription: EXECUTE_TIMESTAMP
        for subscription in subscriptions
    }) == len(subscriptions)


def parse_from_url(method, url, source_type, kwargs):
    content = requests.request(method, url, **kwargs).content
    match source_type:
        case "XML":
            try:
                doc = etree.XML(content)
            except etree.XMLSyntaxError:
                logger.error(f"Failed to parse XML: {url=}")
                return
        case "HTML":
            doc = etree.HTML(content)
            if doc is None:
                logger.error(f"Failed to parse HTML: {url=}")
                return
        case "JSON":
            try:
                doc = json.loads(content)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON: {url=}")
                return
            if not isinstance(doc, dict):
                logger.error(f"Failed to parse JSON: {url=}")
                return
        case _:
            raise ValueError(f"Unsupported source type: {source_type}.")
    return doc


def get_xpath(node, path, source_type):
    match source_type:
        case "XML" | "HTML":
            return node.xpath(path)
        case "JSON":
            return eval(path, {"node": node})


def get_item_sort_key(item, subscription):
    DEFAULT_DEFAULT_SORT_KEY = "0"
    default_sort_key = eval(str(subscription.get("defaultSortKey", DEFAULT_DEFAULT_SORT_KEY)))
    try:
        sort_key_field = subscription.get("sortKey")
        if sort_key_field is not None:
            sort_key = eval(sort_key_field, globals() | item) or default_sort_key
        else:
            sort_key = default_sort_key
    except Exception as e:
        logger.error(f"Failed to eval sort key for an item. Error {e}. Use default key `{default_sort_key}` instead. {item=}")
        sort_key = default_sort_key
    return sort_key


def get_item_id(item ,subscription):
    DEFAULT_ID_FIELD = "link"
    try:
        if not (item_id := str(eval(subscription.get("id", DEFAULT_ID_FIELD), globals() | item))):
            item_id = None
    except Exception as e:
        item_id = None
    if not item_id:
        logger.debug(f"Failed to eval id for an item. Skipped. {item=}")
    return item_id


def fetch_one(config):
    url = config["url"]
    if (doc := parse_from_url(
        config.get("method", "GET"),
        url,
        (source_type := config.get("source_type", "XML")),
        config.get("request_args", {})
    )) is None:
        return
    if source_type == "XML" and len(ttl_node := doc.xpath("/rss/channel/ttl")) == 1 and (ttl := int(ttl_node[0].text)) > (interval := config.get("interval", INTERVAL)):
        logger.warning(f"The recommended interval for this rss source is {ttl} (minutes), while the current interval is {interval} (minutes).")
    item: etree._Element
    for item in get_xpath(doc, config.get("item_xpath", ITEM_XPATH), source_type):
        parsed_item: Dict[str, etree._Element] = {}
        for key, xpath in (FIELDS_XPATH | config.get("xpath", {})).items():
            if xpath is None:
                continue
            if len(_item := get_xpath(item, xpath, source_type)) != 1:
                logger.warning(f"{url=}")
                logger.warning(f"An item has {len(_item)} (!= 1) `{key}` fields.")
                parsed_item[key] = None
            else:
                _item = _item[0]
                if isinstance(_item, etree._Element):
                    parsed_item[key] = _item.text or None
                elif isinstance(_item, str):
                    parsed_item[key] = str(_item)
                else:
                    parsed_item[key] = _item
                if not parsed_item[key]:
                    logger.warning(f"Empty item: {parsed_item[key]!r}. subscription={config.get('name')}, {key=}, {xpath=}")
        yield parsed_item


last_time_send_message = datetime.datetime(1, 1, 1)
last_time_send_message_by_chat = defaultdict(lambda: datetime.datetime(1, 1, 1))


def sleep_until(start_time: datetime.datetime, seconds: float):
    time.sleep(max(0, seconds - (datetime.datetime.now() - start_time).total_seconds()))
    return datetime.datetime.now()


def _send_message(bot_token: str, chat_id: str, text: str, parse_mode: str=""):
    # https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
    global last_time_send_message, last_time_send_message_by_chat
    last_time_send_message = sleep_until(last_time_send_message, 0.05)
    last_time_send_message_by_chat[chat_id] = sleep_until(last_time_send_message_by_chat[chat_id], 3)
    return requests.get(f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={text}&parse_mode={parse_mode}")


def send_message(bot_token: str, chat_id: str, item, config, admin_chat_id: str=""):
    global last_time_send_message

    parse_mode = config.get("parse_mode", "")
    message = EscapeFstringFormatter(parse_mode).format(config.get("message_format", MESSAGE_FORMAT), **(config | item))

    ret = _send_message(bot_token, chat_id, urllib.parse.quote(message), parse_mode)
    if not json.loads(ret.text)["ok"]:
        logger.error(f"Send message to chat `{chat_id}` failed.")
        logger.debug(f"{message=}")
        logger.debug(f"url={ret.url}")
        logger.debug(f"response={ret.text}")
        if admin_chat_id:
            _send_message(bot_token, admin_chat_id, f"Send message to chat `{chat_id}` failed.\nurl={ret.url}\nresponse={ret.text}")


def md5(string: str):
    return hashlib.md5(string.encode("utf-8")).hexdigest()[:8]


def send_all(config):
    bot_token = config.get("bot_token", os.environ.get("BOT_TOKEN"))
    if bot_token is None:
        logger.error("No bot token is given.")
        return
    admin_chat_id = config.get("admin_chat_id", "")

    subscriptions = {}
    for subscription in config.get("subscriptions", []):
        url = subscription.get("url")
        name = subscription.get("name")
        if url is None:
            logger.error("No url for subscription.")
            logger.debug(f"{subscription=}")
        elif name is None:
            logger.error("No name for subscription.")
            logger.debug(f"{subscription=}")
        else:
            subscriptions[name] = subscription

    groups = defaultdict(dict, {s: {s: {}} for s in subscriptions})
    for name, group in config.get("rssgroups", {}).items():
        if name in groups:
            logger.warning(f"Repeated group `{name}`. Will overwrite the previous one.")
        group_config = group.get("config", {})
        for subgroup in group.get("subscriptions", []):
            if subgroup in groups:
                for subscription, _config in groups[subgroup].items():
                    groups[name][subscription] = _config | group_config
            else:
                logger.error(f"Unrecognised subgroup `{subgroup}`. Skipped.")
                logger.debug(f"{name}: {group}")

    messages = {}
    saved_content = r.hgetall(f"saved_content")
    messages_to_send = defaultdict(list)
    for channel, group in config.get("channels", {}).items():
        messages |= {
            subscription: list(fetch_one(subscriptions[subscription]))
            for subscription in groups[group]
            if subscription not in messages and check_interval(subscription, subscriptions[subscription].get("interval", INTERVAL))
        }
        for item, subscription in sorted(sum(
            [
                [
                    (item, subscription)
                    for item in messages[subscription]
                    if (item_id := get_item_id(item, subscriptions[subscription])) and md5(item_id) not in saved_content.get(subscription, '')
                ]
                for subscription in groups[group]
                if subscription in messages
            ],
            []
        ), key=lambda _m: get_item_sort_key(_m[0], groups[group][_m[1]])):
            messages_to_send[channel].append((bot_token, channel, item, subscriptions[subscription] | groups[group][subscription]))

    idx = 0
    logger.debug(f"Number of messages to send: { {channel: len(m) for channel, m in messages_to_send.items()} }")
    while True:
        flag = False
        for channel, _messages in messages_to_send.items():
            if idx < len(_messages):
                send_message(*_messages[idx], admin_chat_id=admin_chat_id)
                flag = True
        if not flag:
            break
        idx += 1

    if messages:
        # update_last_fetch_time(list(messages.keys()))
        r.hset(f"saved_content", mapping={
            s: ":".join([md5(str(get_item_id(_m, subscriptions[s]))) for _m in m])
            for s, m in messages.items()
        })


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
