# coding: utf-8

import os
import json
import requests
import dateutil.parser
import datetime
import hashlib
import math
import random
import yaml
import math
import time
import urllib
from lxml import etree
from typing import Dict
from collections import defaultdict, Counter
from loguru import logger
from const import INTERVAL, ITEM_XPATH, FIELDS_XPATH, MESSAGE_FORMAT, MESSAGE_TYPE, FUNCS, r, source_type_class_map
from common.formatter import EscapeFstringFormatter
from common.merge_dict import merge_dict
from common.get_chat_info import get_chat_info


report = {}


def get_report_string():
    report_string = []
    report_string.append(f"Run at {report['start_at']}.")
    report_string.append("Next fetch time:")
    report_string.extend([
        f"  {item['time'].strftime('%Y-%m-%d %H:%M:%S%z')} fetch?{'✓' if item['fetch'] else '🞨'}: {item['name']}"
        for item in sorted(
            report.get("next_fetch_time", []),
            key=lambda item: item["time"], reverse=False
        )
    ])
    report_string.append(f"Retrieve from: {', '.join(report['feeds_to_fetch'])}")
    if len(report.get('parse_from_url_errors', [])):
        report_string.append(f"Detected errors when parsing: {', '.join([item['name'] for item in report['parse_from_url_errors']])}")
    if len(report.get("field_parsing_failure", [])):
        report_string.extend([f"In feed {name}, {c} items have {num} (!= 1) `{key}` fields." for (name, key, num), c in Counter(report["field_parsing_failure"]).items()])
    if len(report.get("get_item_sort_key_errors", [])):
        report_string.extend([f"{c} items in group {name} failed to evaluate sort key `{sort_key}`. Default: `{default!r}`." for (name, sort_key, default), c in Counter(report["get_item_sort_key_errors"]).items()])
    if len(report["get_feed_item_id_errors"]):
        report_string.append(f"Num of errors when evaluating feed item id: {report['get_feed_item_id_errors']}.")
    if len(report["get_group_item_id_errors"]):
        report_string.append(f"Num of errors when evaluating group item id: {report['get_group_item_id_errors']}.")
    report_string.append(f"Fetching results:")
    report_string.extend([
        f"  {item['num']} items from {item['name']}. Overlapping starts from {item['item_id']}."
        if item["break"] == 1 else
        f"  {item['num']} items from {item['name']}. No overlapping from previous fetch."
        for item in report["num_items"]
    ])
    report_string.append(f"Number of messages to send:")
    report_string.extend([f"  {item['num']} messages of group {item['chat']} to {item['chat_id']} ({get_chat_info(item['chat_id'])})" for item in report["num_messages"]])
    if len(report.get("send_message_errors", [])):
        report_string.append(f"Num of errors when sending messages:")
        report_string.extend([f'{value}: {chat_id} ({get_chat_info(chat_id)})' for chat_id, value in report['send_message_errors'].items()])
    lines = []
    cur_line = ""
    # Split report into several messages each shorter than 4096 characters
    # https://core.telegram.org/bots/api#sendmessage
    for line in report_string:
        if len(cur_line + "\n" + line) <= 4000:
            cur_line += "\n" + line
        else:
            lines.append(cur_line)
            cur_line = line
    if cur_line:
        lines.append(cur_line)
    return [f"Page {idx+1}/{len(lines)}\n{line}" for idx, line in enumerate(lines)]


# Interval of the task (minutes)
TASK_INTERVAL = 60


def filter_feeds_by_interval(intervals):
    current_ts = datetime.datetime.now().timestamp()
    last_fetch_time = r.hgetall("last_fetch_time")

    report["next_fetch_time"] = [
        {
            "name": feed_name,
            # Fetch the feed with a very small prob even if it's not time. Two reasons for this:
            # 1. If you, for example, have 10 feeds all with intervals of 2 hours, this will slowly slowly spread the tasks evenly in every hour, rather than doing everything in one hour and idling in the other.
            # 2. The `last_fetch_time` will be several minutes later than the script starts, if you have a feed with an interval of 2 hours, it will be very likely retrieved only every 3 hours, if we do nothing here.
            "fetch": 1 / (1 + math.exp(- ((current_ts - float(last_fetch_time.get(feed_name) or 0) - interval * 60) / (60 * TASK_INTERVAL) * 10))) > random.random() / 2,
            "time": (datetime.datetime.fromtimestamp(float(last_fetch_time.get(feed_name, '0'))) + datetime.timedelta(minutes=interval)).astimezone(datetime.timezone.utc),
        }
        for feed_name, interval in intervals.items()
    ]

    return set(
        item["name"] for item in report["next_fetch_time"] if item["send"]
    )


def update_last_fetch_time(keys):
    last_fetch_time = datetime.datetime.now().timestamp()
    return len(keys) == 0 or r.hset("last_fetch_time", mapping={
        key: last_fetch_time
        for key in keys
    }) == len(keys)


def parse_from_url(method, url, source_type, kwargs):
    if (scls := source_type_class_map.get(source_type)) is None:
        logger.error(f"Unsupported source type: {source_type}.")
    elif (doc := scls.parse_from_url(scls.get_text(method, url, kwargs))) is None:
        logger.error(f"Failed to parse from {url=}. {source_type=}")
    else:
        return doc


def get_xpath(node, path, source_type):
    return source_type_class_map[source_type].get_xpath(node, path)


def get_feed_items(config):
    if "parse_from_url_errors" not in report:
        report["parse_from_url_errors"] = []
    if (doc := parse_from_url(
        config.get("method", "GET"),
        config["url"],
        (source_type := config.get("source_type", "XML")),
        config.get("request_args", {})
    )) is None:
        report["parse_from_url_errors"].append({
            "name": config["name"],
        })
        return

    if source_type == "XML" and len(ttl_node := doc.xpath("/rss/channel/ttl/text()")) == 1 and ttl_node[0].isdigit() and (ttl := int(ttl_node[0])) > (interval := config.get("interval", INTERVAL)):
        logger.warning(f"The recommended interval for this feed is {ttl} minutes, while the interval you set is {interval} minutes.")

    if "field_parsing_failure" not in report:
        report["field_parsing_failure"] = []
    item: etree._Element
    for item in get_xpath(doc, config.get("item_xpath", ITEM_XPATH), source_type):
        fields: Dict[str, etree._Element] = {}
        for key, xpath in (FIELDS_XPATH | config.get("xpath", {})).items():
            if xpath is None:
                # Can be deliberately set to `None` to skip default fields.
                continue
            if len(field := get_xpath(item, xpath, source_type)) != 1:
                report["field_parsing_failure"].append((
                    config['name'],
                    key,
                    len(field),
                ))
                logger.warning(f"An item from feed {config['name']} (url `{config['url']}`) has {len(field)} (!= 1) `{key}` fields.")
                fields[key] = None
            else:
                fields[key] = field[0]
        yield fields


def get_item_sort_key(item, config):
    default_sort_key = eval(str(config.get("default_sort_key", "0")), FUNCS)
    sort_key_field = config.get("sort_key")
    if "get_item_sort_key_errors" not in report:
        report["get_item_sort_key_errors"] = []
    try:
        if sort_key_field is not None:
            sort_key = eval(sort_key_field, FUNCS | item) or default_sort_key
        else:
            sort_key = default_sort_key
    except Exception as e:
        report["get_item_sort_key_errors"].append((
            config["name"],
            sort_key_field,
            default_sort_key,
        ))
        logger.error(f"Failed to eval sort key for a feed in group {config['name']}. Error `{e}`. Use default key `{default_sort_key}` instead. {item=}, {sort_key_field=}")
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

    if "send_message_errors" not in report:
        report["send_message_errors"] = Counter()
    if not (ret_json := json.loads(ret.text))["ok"]:
        logger.error(f"Send {message_type} to chat `{chat_id}` failed.")
        logger.debug(f"{message_args=}")
        logger.debug(f"url={ret.url}")
        logger.debug(f"response={ret.text}")
        if ret_json.get("error_code") == 429 and isinstance((retry_after := ret_json.get("parameters", {}).get("retry_after")), int):
            time.sleep(retry_after)
            return send_message(bot_token, chat_id, item, config, admin_chat_id)
        report["send_message_errors"][chat_id] += 1
        # if admin_chat_id:
        #     _send_message(bot_token, admin_chat_id, text=f"Send {message_type} to chat `{chat_id}` failed.\nurl={ret.url}\nresponse={ret.text}")


def md5(string: str):
    return hashlib.md5(string.encode("utf-8")).hexdigest()[:8]


def send_all(config):
    report["start_at"] = datetime.datetime.now().astimezone(datetime.timezone.utc)

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

        feed_fields = set(["name", "url", "id", "fields", "expand_from", "interval", "source_type", "method", "request_args", "item_xpath", "xpath",])
        if len(ukn_fields := (set(feed.keys()) - feed_fields)):
            logger.error(f"Feed {name} have unknown fields: {', '.join(ukn_fields)}.")

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

        group_fields = set(["name", "feeds", "message_config", "sort_key", "default_sort_key", "id", "fields",])
        if len(ukn_fields := (set(group.keys()) - group_fields)):
            logger.error(f"Group {name} have unknown fields: {', '.join(ukn_fields)}.")

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
    feeds_to_fetch = filter_feeds_by_interval({
        feed_name: feed.get("interval", INTERVAL)
        for feed_name, feed in feeds.items()
        if "url" in feed
    }) & feeds_to_send

    report["feeds_to_fetch"] = list(feeds_to_fetch)
    report["num_items"] = []
    report["get_feed_item_id_errors"] = Counter()
    for feed_name in feeds_to_fetch:
        feed = feeds[feed_name]
        item_ids = feed_item_ids.get(feed_name, "")
        logger.debug(f"Get feed items from feed {feed_name}")
        for item in get_feed_items(feed):
            item_id = get_item_id(item, feeds[feed_name].get("id"))
            if item_id is None:
                report["get_feed_item_id_errors"][feed_name] += 1
                continue
            hashed_item_id = md5(item_id)
            if hashed_item_id in item_ids:
                report["num_items"].append({"num": len(feed_items[feed_name]), "name": feed_name, "break": 1, "item_id": item_id})
                break
            new_feed_item_ids[feed_name].append(hashed_item_id)
            feed_items[feed_name].append(feed.get("fields", {}) | item)
        else:
            report["num_items"].append({"num": len(feed_items[feed_name]), "name": feed_name, "break": 0})

    report["get_group_item_id_errors"] = Counter()
    send_message_args = defaultdict(list)
    for chat_id, group_name in chats.items():
        item_ids = set()
        for item in sorted([
            {"feed_config": feeds[feed_name], "group_config": group_feed_config} | group_feed_config.get("fields", {}) | item
            for feed_name, group_feed_config in group_feeds[group_name]
            for item in feed_items.get(feed_name, [])
        ], key=lambda item: get_item_sort_key(item, item["group_config"])):
            item_id = get_item_id(item, item["group_config"].get("id", item["feed_config"].get("id")))
            if item_id is None:
                report["get_group_item_id_errors"][group_name] += 1
                continue
            if (hashed_item_id := md5(item_id)) not in item_ids:
                item_ids.add(hashed_item_id)
                send_message_args[chat_id].append((bot_token, chat_id, item, item["group_config"]))

    report["num_messages"] = [{"num": len(m), "chat": chats[chat_id], "feeds": [name for name, _ in group_feeds[chats[chat_id]]], "chat_id": chat_id} for chat_id, m in send_message_args.items()]
    logger.debug(f"Messages to send: {report['num_messages']}")
    for idx in range(max_idx := max([len(m) for chat_id, m in send_message_args.items()], default=0)):
        logger.debug(f"Send messages ... ({idx} / {max_idx})")
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
        for line in get_report_string():
            ret = _send_message(bot_token, admin_chat_id, text=line)
            logger.debug(f"Report response: {ret.text}")


def main():
    send_all(yaml.load(r.get("config"), yaml.Loader))


if __name__ == "__main__":
    main()
