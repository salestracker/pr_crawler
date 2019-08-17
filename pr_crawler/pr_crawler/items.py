# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import re

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, Compose, Join, Identity
from w3lib.html import remove_tags, replace_escape_chars, replace_entities

_UNICODE_SANITIZE_WHITESPACE_REGEX = re.compile(r'[\s+]{2,}')


def sanitize_unicode_whitespace(text):
  '''Escapes extra whitespace and replaces hex space with whitespace.'''
  return _UNICODE_SANITIZE_WHITESPACE_REGEX.sub(' ', text).replace(
      '\xa0', ' ').replace('-', '').strip()


def list_to_string():
  return Compose(Join(), remove_tags, replace_escape_chars,
                 sanitize_unicode_whitespace)


def clean_contact_tags():
  return Compose(lambda x: x[0], remove_tags, replace_escape_chars,
                 sanitize_unicode_whitespace)


def clean_contact_fields():
  return Compose(remove_tags, replace_escape_chars, sanitize_unicode_whitespace,
                 lambda x: x.strip(':'))


class CompanyOverviewItem(scrapy.Item):
  company_overview_uri = scrapy.Field()
  categories_uri = scrapy.Field()
  contacts_uri = scrapy.Field()
  name = scrapy.Field(output_processor=list_to_string())
  company_description = scrapy.Field(
      output_processor=Compose(Join(''), remove_tags, replace_escape_chars,
                               replace_entities, sanitize_unicode_whitespace))
  img = scrapy.Field()
  url = scrapy.Field(output_processor=Compose(lambda url_list: url_list[0]))
  contacts = scrapy.Field()
  status = scrapy.Field(output_processor=list_to_string())
  founded = scrapy.Field(output_processor=list_to_string())
  symbol = scrapy.Field(output_processor=list_to_string())
  annual_revenue = scrapy.Field(output_processor=list_to_string())
  exchange = scrapy.Field(output_processor=list_to_string())
  pr_link_id = scrapy.Field(output_processor=list_to_string())
  employees = scrapy.Field(output_processor=list_to_string())
  website = scrapy.Field()
  categories = scrapy.Field(
      output_processor=MapCompose(replace_escape_chars, replace_entities))


class CompanyOverviewItemLoader(ItemLoader):
  default_input_processor = Identity()


class ContactItem(scrapy.Item):
  title = scrapy.Field()
  contact = scrapy.Field()
  phone = scrapy.Field()
  fax = scrapy.Field()
  email = scrapy.Field()
  address = scrapy.Field()
  website = scrapy.Field()


class ContactItemLoader(ItemLoader):
  default_input_processor = Identity()
  default_output_processor = clean_contact_tags()
