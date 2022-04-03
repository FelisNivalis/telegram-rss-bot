# coding: utf-8

import os
import html
import re
import json
import requests
import dateutil.parser
import datetime
import yaml
import math
import time
from lxml import etree
from typing import Dict
from collections import defaultdict
from loguru import logger
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, GROUP_CONFIG_FIELDS, r


EXECUTE_TIMESTAMP = datetime.datetime.now().timestamp()
SEND_MESSAGE_INTERVAL = 3


def escape_markdown(text: str, version: int = 1) -> str:
    # From https://github.com/python-telegram-bot/python-telegram-bot/blob/92cb6f3ae8d5c3e49b9019a9348d4408135ffc95/telegram/utils/helpers.py#L149
    if int(version) == 1:
        escape_chars = r'_*`['
    elif int(version) == 2:
        escape_chars = r'_*[]()~`>#+-=|{}.!'
    else:
        raise ValueError('Markdown version must be either 1 or 2!')

    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


def check_interval(subscription, interval):
    last_fetch_time = r.hget("last_fetch_time", subscription)
    if not last_fetch_time:
        last_fetch_time = 0
    else:
        last_fetch_time = float(last_fetch_time)
    return EXECUTE_TIMESTAMP - last_fetch_time > interval


def update_last_fetch_time(subscriptions):
    if len(subscriptions) == 0:
        return True
    return r.hset('last_fetch_time', mapping={
        subscription: EXECUTE_TIMESTAMP
        for subscription in subscriptions
    }) == len(subscriptions)


def fetch_one(config):
    url = config["url"]
    try:
        items = (
            etree
            .XML(requests.get(url).content)
            .xpath(config.get("item_xpath", ITEM_XPATH))
        )
    except etree.XMLSyntaxError:
        logger.error(f"Failed to parse XML: {config=}")
        return
    item: etree._Element
    for item in items:
        parsed_item: Dict[str, etree._Element] = {}
        for key, xpath in (FIELDS_XPATH | config.get("fields_xpath", {})).items():
            _item = item.xpath(xpath)
            if len(_item) != 1:
                logger.warning(f"{url=}")
                logger.warning(f"An item has {len(_item)} (!= 1) `{key}` fields.")
                parsed_item[key] = ""
            else:
                parsed_item[key] = _item[0].text
        try:
            pub_timestamp = dateutil.parser.parse(parsed_item["pubDate"]).timestamp()
        except dateutil.parser.ParserError:
            logger.error("Failed to parse `pubDate`.")
            logger.debug(f"pubDate={parsed_item['pubDate']}")
            pub_timestamp = 0
        yield pub_timestamp, parsed_item


def send_message(bot_token: str, chat_id: str, item, config):
    time.sleep(0.05)
    args = config | item
    parse_mode = config.get("parse_mode", "")
    args = {
        k: (
            escape_markdown(v, version=2) if parse_mode == "MarkdownV2" else
            escape_markdown(v) if parse_mode == "Markdown" else
            html.escape(v) if parse_mode == "HTML" else
            v
        )
        for k, v in args.items()
        if isinstance(v, str)
    }
    message = config.get("message_format", MESSAGE_FORMAT).format(**args)
    ret = requests.get(f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}&parse_mode={config.get('parse_mode', '')}")
    if not json.loads(ret.text)["ok"]:
        logger.error(f"Send message to chat `{chat_id}` failed.")
        logger.debug(f"{message=}")
        logger.debug(f"ret={ret.text}")


def send_all(config):
    bot_token = config.get("BOT_TOKEN", os.environ.get("BOT_TOKEN"))
    if bot_token is None:
        logger.error("No bot token is given.")
        return

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
                logger.error(f"Unrecognised subgroup `{subgroup}`")
                logger.debug(f"{name}: {group}")

    messages = {}
    lasttimestamp = r.hgetall(f"lasttimestamp")
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
                    (t, item, subscription)
                    for t, item in messages[subscription]
                    if t >= lasttimestamp.get(subscription, 0)
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
        time_start = datetime.datetime.now()
        for channel, _messages in messages_to_send.items():
            if idx < len(_messages):
                send_message(*_messages[idx])
                flag = True
        if not flag:
            break
        # https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
        time.sleep(max(0, 3 - (datetime.datetime.now() - time_start).total_seconds()))
        idx += 1

    update_last_fetch_time(list(messages.keys()))
    if messages:
        r.hset(f"lasttimestamp", mapping={
            s: res
            for s, m in messages.items()
            if m and (res := max([_m[0] for _m in m])) > 0
        })


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
