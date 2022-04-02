# coding: utf-8

import os
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


EXECUTE_SECONDS = (datetime.datetime.now() - datetime.datetime.combine(datetime.date.today(), datetime.time())).seconds


def check_interval(interval):
    # allows for at most 60 seconds error
    idx = EXECUTE_SECONDS / interval
    return math.fabs(idx - round(idx)) * interval < 60


def fetch_one(config):
    url = config["url"]
    item: etree._Element
    for item in (
        etree
        .XML(requests.get(url).content)
        .xpath(config.get("item_xpath", ITEM_XPATH))
    ):
        parsed_item: Dict[str, etree._Element] = {}
        for key, xpath in (FIELDS_XPATH | config.get("fields_xpath", {})).items():
            _item = item.xpath(xpath)
            if len(_item) != 1:
                logger.warning(f"{url=}")
                logger.warning(f"An item has {len(_item)} (!= 1) `{key}` fields.")
            parsed_item[key] = _item[0].text
        try:
            pub_timestamp = dateutil.parser.parse(parsed_item["pubDate"]).timestamp()
        except dateutil.parser.ParserError:
            logger.error("Failed to parse `pubDate`.")
            logger.debug(f"pubDate={parsed_item['pubDate']}")
            pub_timestamp = 0
        yield pub_timestamp, parsed_item


def send_message(bot_token: str, chat_id: str, item, config):
    message = config.get("message_format", MESSAGE_FORMAT).format(**item)
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
        if not set(group.config.keys()).issubset(GROUP_CONFIG_FIELDS):
            logger.error("Group has invalid config fields: {}".format(", ".join(set(group.config.keys() - GROUP_CONFIG_FIELDS))))
            logger.debug(f"{name}: {group}")
            continue
        for subgroup in group.get("subscriptions", []):
            if subgroup in groups:
                for subscription, config in groups[subgroup].items():
                    groups[name][subscription] = config | group_config
            else:
                logger.error(f"Unrecognised subgroup `{subgroup}`")
                logger.debug(f"{name}: {group}")

    messages = {}
    lasttimestamp = r.hgetall(f"lasttimestamp")
    for channel, group in config.get("channels", {}).items():
        # TODO: interval
        messages |= {
            subscription: list(fetch_one(subscriptions[subscription]))
            for subscription in groups[group]
            if subscription not in messages and check_interval(subscriptions[subscription].get("interval", INTERVAL))
        }
        for _, item, config in sorted(sum(
            [
                [
                    (t, item, groups[group][subscription])
                    for t, item in messages[subscription]
                    if t >= lasttimestamp.get(subscription, 0)
                ]
                for subscription in groups[group]
            ],
            []
        )):
            send_message(bot_token, chat_id, item, config)

    r.hmset(f"lasttimestamp:{channel}", {
        s: max(m, key=lambda _m: _m[0])
        for s, m in messages.items()
    })


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
