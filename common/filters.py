# coding: utf-8

import re
import urllib


def get_iv_url(url, rhash):
    return f"https://t.me/iv?url={urllib.parse.quote(url)}&{rhash=}"
