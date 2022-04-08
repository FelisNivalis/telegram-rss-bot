# coding: utf-8

import html
import re
import string
from loguru import logger
from common.filters import * # noqa


def escape_markdown(text: str, version: int = 1) -> str:
    # From https://github.com/python-telegram-bot/python-telegram-bot/blob/92cb6f3ae8d5c3e49b9019a9348d4408135ffc95/telegram/utils/helpers.py#L149
    if int(version) == 1:
        escape_chars = r'_*`['
    elif int(version) == 2:
        escape_chars = r'_*[]()~`>#+-=|{}.!'
    else:
        raise ValueError('Markdown version must be either 1 or 2!')

    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


class EscapeFstringFormatter(string.Formatter):

    def __init__(self, mtype, kwargs=None):
        match mtype:
            case "Markdown":
                self.escape_func = lambda s: escape_markdown(str(s))
            case "MarkdownV2":
                self.escape_func = lambda s: escape_markdown(str(s), 2)
            case "HTML":
                self.escape_func = lambda s: html.escape(str(s))
            case "":
                self.escape_func = lambda s: s
            case _:
                raise ValueError(f"Unsupported message type `{mtype}`")
        if kwargs is None:
            kwargs = globals()
        self.kwargs = kwargs

    def get_field(self, field_name, args, kwargs):
        if args:
            raise ValueError("`EscapeFstringFormatter` does not support positional arguments.")
        return eval(field_name, self.kwargs | kwargs), field_name

    def convert_field(self, value, convension):
        if convension == "n":
            return value
        return self.escape_func(super().convert_field(value, convension))
