# coding: utf-8

from lxml import etree
import json
import requests


class SourceTypeHTTPRequest():

    @classmethod
    def get_text(cls, method, url, kwargs):
        try:
            text = requests.request(method, url, **kwargs).text
        except requests.exceptions.RequestException:
            return


class SourceTypeXML(SourceTypeHTTPRequest):

    @classmethod
    def parse_from_url(cls, text):
        try:
            return etree.fromstring(text.encode("utf-8"), etree.XMLParser(encoding="utf-8"))
        except etree.XMLSyntaxError:
            return

    @classmethod
    def get_xpath(cls, node, path):
        return node.xpath(path)


class SourceTypeHTML(SourceTypeHTTPRequest):

    @classmethod
    def parse_from_url(cls, text):
        return etree.fromstring(text.encode("utf-8"), etree.HTMLParser(encoding="utf-8"))

    @classmethod
    def get_xpath(cls, node, path):
        return node.xpath(path)


class SourceTypeJSON(SourceTypeHTTPRequest):

    @classmethod
    def parse_from_url(cls, text):
        try:
            doc = json.loads(text)
            if isinstance(doc, dict):
                return doc
        except json.JSONDecodeError:
            return

    @classmethod
    def get_xpath(cls, node, path):
        return eval(path, {"node": node})
