# coding: utf-8

import json
import uuid
import yaml
import logging
from flask import Flask, request
from const import r, WEBHOOK_TOKEN
from functools import wraps

app = Flask(__name__)
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)


def verificate(*args, **kwargs):
    def _v(func):
        @wraps(func)
        def _func(token: str="", *args, **kwargs):
            if token == "":
                code = uuid.uuid4().hex
                r.set("verification_code", code, ex=30)
                app.logger.info("Your verification code: {}, add `/YOUR-CODE` after the url to continue", code)
                return "You need a verification code to continue. Please check your app's logs (you can find the logs at `https://dashboard.heroku.com/apps/YOUR-APPS-NAME/logs`) and add `/YOUR-CODE` after the url to get the chats. The verification code will expire in 60 seconds."
            else:
                if token != r.get("verification_code"):
                    return "Wrong verification code. :(\nNote that each verification code can only be used once."
                else:
                    r.delete("verification_code")
            return func(*args, **kwargs)
        return _func
    return _v


@app.route(f"/updateHook/{WEBHOOK_TOKEN}", methods=["POST"])
def update_hook():
    if (my_chat_member := request.get_json().get("my_chat_member")) is not None:
        chat = my_chat_member["chat"]
        new_chat_member = my_chat_member["new_chat_member"]
        match new_chat_member["status"]:
            case "creator" | "member":
                can_send_message = True
            case "left" | "kicked":
                can_send_message = False
            case "administrator":
                can_send_message = new_chat_member.get("can_post_messages")
            case "restricted":
                can_send_message = new_chat_member["can_send_messages"]
        chat_id = str(chat["id"])
        if can_send_message:
            r.hset("chats", chat_id, json.dumps({
                k: v
                for k, v in chat.items()
                if k in [
                    "id", "type", "title", "username", "first_name", "last_name"
                ]
            }))
        else:
            r.hdel("chats", chat_id)
    return "ok"


@app.route("/getChats", methods=["GET"])
@app.route("/getChats/<string:token>", methods=["GET"])
@verificate()
def get_chats():
    return yaml.dump({
        k: json.loads(v)
        for k, v in r.hgetall("chats").items()
    }, allow_unicode=True)


@app.route("/getConfig", methods=["GET"])
@app.route("/getConfig/<string:token>", methods=["GET"])
@verificate()
def get_config():
    return yaml.dump(yaml.load(r.get("config"), yaml.Loader), allow_unicode=True)


@app.route("/getConfig", methods=["GET"])
@app.route("/setConfig/<string:token>", methods=["POST"])
@verificate()
def set_config():
    data = request.get_data(as_text=True)
    try:
        yaml.load(data, yaml.Loader)
    except yaml.parser.ParserError as e:
        return "Cannot parse the posted data.\n{}".format(e)
    if r.set("config", data):
        return "ok"
    else:
        return "error"
