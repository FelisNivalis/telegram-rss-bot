# coding: utf-8

from lxml import etree
import json


class SourceTypeXML():

    @classmethod
    def parse_from_url(cls, text):
        try:
            return etree.XML(text.encode("utf-8"))
        except etree.XMLSyntaxError:
            return

    @classmethod
    def get_xpath(cls, node, path):
        return node.xpath(path)


class SourceTypeHTML():

    @classmethod
    def parse_from_url(cls, text):
        return etree.HTML(text)

    @classmethod
    def get_xpath(cls, node, path):
        return node.xpath(path)


class SourceTypeJSON():

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


source_type_class_map = {
    "XML": SourceTypeXML,
    "HTML": SourceTypeHTML,
    "JSON": SourceTypeJSON
}
