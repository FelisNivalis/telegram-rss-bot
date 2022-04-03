# coding: utf-8

import os
import re
import json
import requests
import dateutil.parser
import datetime
import yaml
import math
from lxml import etree
from typing import Dict
from collections import defaultdict
from loguru import logger
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, GROUP_CONFIG_FIELDS, r


EXECUTE_TIMESTAMP = datetime.datetime.now().timestamp()


def escape_markdown(text: str, version: int = 1, entity_type: str = None) -> str:
    """
    https://github.com/python-telegram-bot/python-telegram-bot/blob/92cb6f3ae8d5c3e49b9019a9348d4408135ffc95/telegram/utils/helpers.py#L149
    Helper function to escape telegram markup symbols.
    Args:
        text (:obj:`str`): The text.
        version (:obj:`int` | :obj:`str`): Use to specify the version of telegrams Markdown.
            Either ``1`` or ``2``. Defaults to ``1``.
        entity_type (:obj:`str`, optional): For the entity types ``PRE``, ``CODE`` and the link
            part of ``TEXT_LINKS``, only certain characters need to be escaped in ``MarkdownV2``.
            See the official API documentation for details. Only valid in combination with
            ``version=2``, will be ignored else.
    """
    if int(version) == 1:
        escape_chars = r'_*`['
    elif int(version) == 2:
        if entity_type in ['pre', 'code']:
            escape_chars = r'\`'
        elif entity_type == 'text_link':
            escape_chars = r'\)'
        else:
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
        logger.error("Failed to parse XML: {config=}")
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
    args = config | item
    parse_mode = config.get("parse_mode", "")
    if parse_mode == "MarkdownV2":
        args = {k: escape_markdown(v, version=2) for k, v in args.items() if isinstance(v, str)}
    elif parse_mode == "Markdown":
        args = {k: escape_markdown(v) for k, v in args.items() if isinstance(v, str)}
    else:
        args = {k: v for k, v in args.items() if isinstance(v, str)}
    message = config.get("message_format", MESSAGE_FORMAT).format(**args)
    ret = requests.get(f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}&parse_mode={config.get('parse_mode', '')}")
    if not json.loads(ret.text)["ok"]:
        logger.error(f"Send message to chat `{chat_id}` failed.")
        logger.debug(f"{message=}")
        logger.debug(f"ret={ret.text}")
    else:
        logger.debug(f"Sending message to chat `{chat_id}` succeeded.")
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
            send_message(bot_token, channel, item, subscriptions[subscription] | groups[group][subscription])

    update_last_fetch_time(list(messages.keys()))
    if messages:
        r.hset(f"lasttimestamp", mapping={
            s: max(m, key=lambda _m: _m[0])
            for s, m in messages.items()
        })


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
