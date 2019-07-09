# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import re

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Compose, Join, \
  Identity
from w3lib.html import remove_tags, replace_escape_chars, replace_entities

_UNICODE_SANITIZE_WHITESPACE_REGEX = re.compile(r'[\s+]{2,}')


class PrCrawlerItem(scrapy.Item):
  # define the fields for your item here like:
  # name = scrapy.Field()
  pass


def sanitize_unicode_whitespace(text):
  '''Escapes extra whitespace and replaces hex space with whitespace.'''
  return _UNICODE_SANITIZE_WHITESPACE_REGEX.sub(
    ' ', text).replace('\xa0', ' ').strip()


class CompanyOverviewItem(scrapy.Item):
  name = scrapy.Field()
  company_description = scrapy.Field(
    output_processor=Compose(Join(''), remove_tags, replace_escape_chars,
                             replace_entities, sanitize_unicode_whitespace)
  )
  img = scrapy.Field(
    output_processor=Compose(lambda l: l[0], replace_escape_chars)
  )
  phone = scrapy.Field()
  fax = scrapy.Field()
  email = scrapy.Field()
  status = scrapy.Field()
  founded = scrapy.Field()
  symbol = scrapy.Field()
  annual_revenue = scrapy.Field()
  exchange = scrapy.Field()
  pr_link_id = scrapy.Field()
  employees = scrapy.Field()
  website = scrapy.Field()
  industries = scrapy.Field()


class CompanyOverviewItemLoader(ItemLoader):
  default_input_processor = Identity()
