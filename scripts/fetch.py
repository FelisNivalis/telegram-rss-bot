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
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, MESSAGE_TYPE, FUNCS, r
from common.formatter import EscapeFstringFormatter
from common.merge_dict import merge_dict
from source_type import source_type_class_map


report = {}


def get_report_string():
    report_string = []
    report_string.append(f"Run at {report['start_at']}.")
    report_string.append("Next fetch time:")
    report_string.extend([
        f"  {item['name']}: {item['time']}"
        for item in sorted(
            report.get("next_fetch_time", []),
            key=lambda item: item["time"], reverse=True
        )
    ])
    report_string.append(f"Feeds attached to at least one chat: {', '.join(report['feeds_to_send'])}")
    report_string.append(f"Retrieve from: {', '.join(report['feeds_to_fetch'])}")
    report_string.append(f"Fetching results:")
    report_string.extend([
        f"  {item['num']} items from {item['name']}. Overlapping starts from {item['item_id']}."
        if item["break"] == 1 else
        f"  {item['num']} items from {item['name']}. No overlapping from previous fetch."
        for item in report["num_items"]
    ])
    report_string.append(f"Number of messages to send:")
    report_string.extend([f"  {item['num']} messages of group {item['chat']} (feeds: {', '.join(item['feeds'])}) to {item['chat_id']}" for item in report["num_messages"]])
    return '\n'.join(report_string)


def filter_feeds_by_interval(intervals):
    current_ts = datetime.datetime.now().timestamp()
    last_fetch_time = r.hgetall("last_fetch_time")

    report["next_fetch_time"] = [
        {
            "name": feed_name,
            "time": datetime.datetime.fromtimestamp(float(last_fetch_time.get(feed_name, '0'))) + datetime.timedelta(minutes=interval),
        }
        for feed_name, interval in intervals.items()
    ]

    return set(
        feed_name
        for feed_name, interval in intervals.items()
        if current_ts - float(last_fetch_time.get(feed_name) or 0) > interval * 60
    )


def update_last_fetch_time(keys):
    last_fetch_time = datetime.datetime.now().timestamp()
    return len(keys) == 0 or r.hset("last_fetch_time", mapping={
        key: last_fetch_time
        for key in keys
    }) == len(keys)


def parse_from_url(method, url, source_type, kwargs):
    text = requests.request(method, url, **kwargs).text
    scls = source_type_class_map.get(source_type)
    if scls is None:
        logger.error(f"Unsupported source type: {source_type}.")
    elif (doc := scls.parse_from_url(text)) is None:
        logger.error(f"Failed to parse from {url=}. {source_type=}")
    else:
        return doc


def get_xpath(node, path, source_type):
    return source_type_class_map[source_type].get_xpath(node, path)


def get_feed_items(config):
    if (doc := parse_from_url(
        config.get("method", "GET"),
        config["url"],
        (source_type := config.get("source_type", "XML")),
        config.get("request_args", {})
    )) is None:
        return

    if source_type == "XML" and len(ttl_node := doc.xpath("/rss/channel/ttl/text()")) == 1 and ttl_node[0].isdigit() and (ttl := int(ttl_node[0])) > (interval := config.get("interval", INTERVAL)):
        logger.warning(f"The recommended interval for this feed is {ttl} minutes, while the interval you set is {interval} minutes.")

    item: etree._Element
    for item in get_xpath(doc, config.get("item_xpath", ITEM_XPATH), source_type):
        fields: Dict[str, etree._Element] = {}
        for key, xpath in (FIELDS_XPATH | config.get("xpath", {})).items():
            if xpath is None:
                # Can be deliberately set to `None` to skip default fields.
                continue
            if len(field := get_xpath(item, xpath, source_type)) != 1:
                logger.warning(f"An item from url `{config['url']}` has {len(field)} (!= 1) `{key}` fields.")
                fields[key] = None
            else:
                fields[key] = field[0]
        yield fields


def get_item_sort_key(item, config):
    default_sort_key = eval(str(config.get("default_sort_key", "0")), FUNCS)
    sort_key_field = config.get("sort_key")
    try:
        if sort_key_field is not None:
            sort_key = eval(sort_key_field, FUNCS | item) or default_sort_key
        else:
            sort_key = default_sort_key
    except Exception as e:
        logger.error(f"Failed to eval sort key for a feed. Error `{e}`. Use default key `{default_sort_key}` instead. {item=}, {sort_key_field=}")
        sort_key = default_sort_key
    return sort_key


def get_item_id(item, id_field):
    try:
        if not (item_id := str(eval(id_field or "link", FUNCS | item))):
            item_id = None
    except Exception as e:
        item_id = None
    if not item_id:
        logger.debug(f"Failed to eval id for an item. Skipped. {item=}")
    return item_id


last_time_send_message = datetime.datetime(1, 1, 1)
last_time_send_message_by_chat = defaultdict(lambda: datetime.datetime(1, 1, 1))


def sleep_until(until: datetime.datetime):
    while (now := datetime.datetime.now()) < until:
        time.sleep((until - now).total_seconds())
    return datetime.datetime.now()


def _send_message(bot_token: str, chat_id: str, message_type: str=MESSAGE_TYPE, **kwargs):
    # https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
    global last_time_send_message, last_time_send_message_by_chat
    last_time_send_message = sleep_until(last_time_send_message + datetime.timedelta(seconds=0.05))
    last_time_send_message_by_chat[chat_id] = sleep_until(last_time_send_message_by_chat[chat_id] + datetime.timedelta(seconds=3))
    return requests.get(f"https://api.telegram.org/bot{bot_token}/send{message_type}", params={"chat_id": chat_id} | kwargs)


def send_message(bot_token: str, chat_id: str, item, config, admin_chat_id: str=""):
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

    if not json.loads(ret.text)["ok"]:
        logger.error(f"Send {message_type} to chat `{chat_id}` failed.")
        logger.debug(f"{message_args=}")
        logger.debug(f"url={ret.url}")
        logger.debug(f"response={ret.text}")
        if admin_chat_id:
            _send_message(bot_token, admin_chat_id, text=f"Send {message_type} to chat `{chat_id}` failed.\nurl={ret.url}\nresponse={ret.text}")


def md5(string: str):
    return hashlib.md5(string.encode("utf-8")).hexdigest()[:8]


def send_all(config):
    report["start_at"] = datetime.datetime.now()

    bot_token = config.get("bot_token", os.environ.get("BOT_TOKEN"))
    if bot_token is None:
        logger.error("No bot token is given.")
        return

    admin_chat_id = config.get("admin_chat_id", "")

    feeds = {}
    for feed in config.get("feeds", []):
        if (name := feed.get("name")) is None:
            logger.error("No name for the feed.")
            logger.debug(f"{feed=}")
            continue
        expand_from = feed.get("expand_from")
        if expand_from is not None:
            if not isinstance(expand_from, list):
                logger.warning(f"`expand_from` of feed {name} is of type `{type(expand_from)}`, should be a list.")
            else:
                for from_feed in reversed(expand_from):
                    if from_feed not in feeds:
                        logger.error(f"Unknown feed {from_feed} to expand from. Asked by {feed.get('name')}.")
                        logger.debug(f"{feed=}")
                    else:
                        feed = merge_dict(feeds[from_feed], feed)

        feeds[name] = feed

    group_feeds = defaultdict(list, {name: [(name, {})] for name, feed in feeds.items() if "url" in feed})
    for group in config.get("rssgroups", []):
        group_name = group.get("name")
        if not group_name:
            logger.error(f"The group does not have a name. {group=}")
            continue
        if group_name in group_feeds:
            logger.warning(f"Repeat group `{group_name}`. Will append the previous one.")

        if not group.get("feeds"):
            logger.warning(f"No feeds in group `{group_name}`.")
            continue
        for group_feed in group.get("feeds", []):
            if group_feed in group_feeds:
                group_feeds[group_name] += [(_name, merge_dict(_config, group)) for _name, _config in group_feeds[group_feed]]
            else:
                logger.error(f"Unrecognised feed `{group_feed}` in group `{group_name}`. Skipped.")

    # Start to send...
    feed_item_ids = r.hgetall(f"feed_item_ids")
    new_feed_item_ids = defaultdict(list)
    feed_items = defaultdict(list)
    chats = config.get("chats", {})
    feeds_to_send = set([name for group in chats.values() for name, config in group_feeds[group]])
    report["feeds_to_send"] = feeds_to_send
    feeds_to_fetch = filter_feeds_by_interval({
        feed_name: feed.get("interval", INTERVAL)
        for feed_name, feed in feeds.items()
        if "url" in feed
    }) & feeds_to_send

    report["feeds_to_fetch"] = list(feeds_to_fetch)
    report["num_items"] = []
    for feed_name in feeds_to_fetch:
        feed = feeds[feed_name]
        item_ids = feed_item_ids.get(feed_name, "")
        for item in get_feed_items(feed):
            item_id = get_item_id(item, feeds[feed_name].get("id"))
            hashed_item_id = md5(item_id)
            if hashed_item_id in item_ids:
                report["num_items"].append({"num": len(feed_items[feed_name]), "name": feed_name, "break": 1, "item_id": item_id})
                break
            new_feed_item_ids[feed_name].append(hashed_item_id)
            feed_items[feed_name].append(feed.get("fields", {}) | item)
        else:
            report["num_items"].append({"num": len(feed_items[feed_name]), "name": feed_name, "break": 0})

    send_message_args = defaultdict(list)
    for chat_id, group_name in chats.items():
        logger.debug(f"get_item_sort_key {chat_id}, {group_name}")
        logger.debug(', '.join([
            "{}, {}".format(item, get_item_sort_key({"feed_config": feeds[feed_name], "group_config": group_feed_config} | group_feed_config.get("fields", {}) | item, group_feed_config))
            for feed_name, group_feed_config in group_feeds[group_name]
            for item in feed_items.get(feed_name, [])
        ]))
        item_ids = set()
        for item in sorted([
            {"feed_config": feeds[feed_name], "group_config": group_feed_config} | group_feed_config.get("fields", {}) | item
            for feed_name, group_feed_config in group_feeds[group_name]
            for item in feed_items.get(feed_name, [])
        ], key=lambda item: get_item_sort_key(item, item["group_config"])):
            if (hashed_item_id := md5(get_item_id(item, item["group_config"].get("id", item["feed_config"].get("id"))))) not in item_ids:
                item_ids.add(hashed_item_id)
                send_message_args[chat_id].append((bot_token, chat_id, item, item["group_config"]))

    report["num_messages"] = [{"num": len(m), "chat": chats[chat_id], "feeds": [name for name, _ in group_feeds[chats[chat_id]]], "chat_id": chat_id} for chat_id, m in send_message_args.items()]
    for idx in range(max([len(m) for chat_id, m in send_message_args.items()], default=0)):
        for chat_id, _messages in send_message_args.items():
            if idx < len(_messages):
                send_message(*_messages[idx], admin_chat_id=admin_chat_id)

    update_last_fetch_time(feeds_to_fetch)
    if new_feed_item_ids:
        r.hset(f"feed_item_ids", mapping={
            key: ":".join(ids)
            for key, ids in new_feed_item_ids.items()
            if ids
        })

    if admin_chat_id:
        _send_message(bot_token, admin_chat_id, text=get_report_string())


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
