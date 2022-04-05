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
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, GROUP_CONFIG_FIELDS, r
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
                parsed_item[key] = ""
            else:
                _item = _item[0]
                if isinstance(_item, etree._Element):
                    parsed_item[key] = _item.text or ""
                elif isinstance(_item, str):
                    parsed_item[key] = str(_item)
                else:
                    logger.warning(f"Unknown item type: {type(_item)}, item={_item}. {url=}, {key=}, {xpath=}")
                    parsed_item[key] = ""
        if (item_id := parsed_item.get("id", parsed_item.get("link"))):
            yield item_id, parsed_item
        else:
            logger.warning(f"The item does not have an id. Skipped. RSS {url=}, item={etree.tostring(item)}")


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
        if admin_chat_id:
            _send_message(bot_token, admin_chat_id, f"Send message to chat `{chat_id}` failed.\nurl={ret.url}\nresponse={ret.text}")
    else:
        logger.debug(f"Send message to chat `{chat_id}` succeeded.")
    logger.debug(f"{message=}")
    logger.debug(f"url={ret.url}")
    logger.debug(f"response={ret.text}")


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
        if not set(group_config.keys()).issubset(GROUP_CONFIG_FIELDS):
            logger.error("Group has invalid config fields: {}".format(", ".join(set(group_config.keys() - GROUP_CONFIG_FIELDS))))
            logger.debug(f"{name}: {group}")
            continue
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
        for _, item, subscription in sorted(sum(
            [
                [
                    (link, item, subscription)
                    for link, item in messages[subscription]
                    if str(md5(link)) not in saved_content.get(subscription, '')
                ]
                for subscription in groups[group]
                if subscription in messages
            ],
            []
        ), key=lambda _m: _m[0]):
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

    update_last_fetch_time(list(messages.keys()))
    if messages:
        r.hset(f"saved_content", mapping={
            s: ":".join([str(md5(_m[0])) for _m in m])
            for s, m in messages.items()
        })


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
