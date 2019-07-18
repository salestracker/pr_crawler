# -*- coding: utf-8 -*-
import urllib
from collections import defaultdict

from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import CompanyOverviewItem, CompanyOverviewItemLoader, \
  ContactItem, ContactItemLoader, list_to_string

_BASE_URI = 'https://www.pr.com'
_BIZ_LINKS = [
  r'business-directory/*',
]

_COMPANY_LINK = [
  r'company-profile/overview/[a-zA-Z0-9]+',
]

_DENY_LINKS = [
  r'company-profile/industries/*',
  r'company-profile/press-releases/*',
  r'company-profile/contact-info/*',
  r'company-profile/products-services/*',
  r'company-profile/employees-list/*',
  r'company-profile/employees/*',
  r'company-profile/employees-biography/*',
  r'company-profile/awards/*',
  r'company-profile/investment-highlights/*',
]

_COMPANY_FIELDS_COMMON = r'//table[3]//table'
_COMPANY_FIELDS_1 = {
  'name': '//table[2]//table//table[1]//tr[3]//h1/text()',
  'company_description': ('//table[2]/descendant::table[contains(name(), '
                          'tbody)]/descendant::div/div[2]/text()'),
  'img': ('//table[2]/descendant::table/descendant::table[1]'
          '[contains(name(),tr[3])][contains(name(), '
          'h1)]//img/@src[starts-with(., "https")]|//table//tr['
          '2]//img/@src[starts-with(., "https")]'),
}
_COMPANY_FIELDS_2 = {
  'status': r'//tr[1]/td[2]/text()',
  'founded': r'//tr[1]/td[4]/text()',
  'annual_revenue': r'//tr[1]/td[6]/text()',
  'exchange': r'//tr[2]/td[2]/text()',
  'pr_link_id': r'//tr[2]/td[4]/text()',
  'employees': r'//tr[2]/td[6]/text()',
  'symbol': r'//tr[3]/td[2]/text()',
  'website': r'//tr[3]/td[4]/text()',
  'url': '//table[2]//table//table[3]//tr/td//a[1]/@href[position()=1]',
}

_COMPANY_CATEGORIES_URI = '//table[2]//table//table[3]//tr/td//i/a/@href'
_CATEGORIES_PARSE_XPATH = ('//table[2]//tr[1]//table//tr[2]/td[2]//table['
                           '3]//tr/td/div/a/text()')

_COMPANY_CONTACT_URI = ('//table[2]//tr[1]/td/table//tr[2]/td[2]/table['
                        '1]//tr[2]/td/table//tr/td[2]/table//tr/td['
                        '3]/table//tr/td[2]/h3/a/@href')
_CONTACT_PARSE_XPATH = ('//table[2]//tr[1]/td/table//tr[2]/td[2]/table['
                        '3]//tr/td/table//tr[*]/td/table//tr/td')
                        


class ParseDirectorySpider(CrawlSpider):
  name = 'parse_directory'
  allowed_domains = ['pr.com']
  start_urls = [r'https://www.pr.com/business-directory/']
  
  rules = (
    Rule(
      LinkExtractor(allow=_BIZ_LINKS, deny=_DENY_LINKS),
      callback=None,
      follow=True),
    Rule(
      LinkExtractor(allow=_COMPANY_LINK, deny=_DENY_LINKS),
      callback='parse_item',
      follow=False),
  )
  
  def parse_item(self, response):
    '''Parses company details.
    
    :param response:
    :return:
    '''
    # print(response.xpath(_START_XPATH))
    item = CompanyOverviewItem()
    item_loader = CompanyOverviewItemLoader(item=item, response=response)
    for key in _COMPANY_FIELDS_1:
      item_loader.add_xpath(key, _COMPANY_FIELDS_1[key])
    field_loader = item_loader.nested_xpath(_COMPANY_FIELDS_COMMON)
    for key in _COMPANY_FIELDS_2:
      field_loader.add_xpath(key, _COMPANY_FIELDS_2[key])
    category_href = response.xpath(_COMPANY_CATEGORIES_URI).get()
    contact_href = response.xpath(_COMPANY_CONTACT_URI).get()
    category_attributes = ['categories']
    contact_attributes = []
    href_dict = {category_href: (category_attributes,
                                 _CATEGORIES_PARSE_XPATH, 'parse_attribute'),
                 contact_href: (contact_attributes, _CONTACT_PARSE_XPATH,
                                'parse_contact')}
    item_dict = item_loader.load_item()
    for href in href_dict:
      uri = urllib.parse.urljoin(_BASE_URI, href)
      yield Request(uri, callback=getattr(self, href_dict[href][2]),
                    meta={
                      'item': item,
                      'item_dict': item_dict,
                      'attributes': href_dict[href][0],
                      'parse_xpath': href_dict[href][1],
                    })
    return item_dict
  
  def parse_attribute(self, response):
    item = response.meta['item']
    item_dict = response.meta['item_dict']
    attributes = response.meta['attributes']
    parse_xpath = response.meta['parse_xpath']
    item_loader = CompanyOverviewItemLoader(item=item, response=response)
    parsed_values = response.xpath(parse_xpath).getall()
    matched_values = (zip(attributes, parsed_values) if len(attributes) ==
                                                        len(parsed_values)
                      else [(attributes[0], parsed_values)])
    for attribute, value in matched_values:
      if value:
        item_loader.add_value(attribute, value)
    item_dict.update(item_loader.load_item())
    return item_dict

  def parse_contact(self, response):
    item = response.meta['item']
    item_dict = response.meta['item_dict']
    parse_xpath = response.meta['parse_xpath']
    item_loader = CompanyOverviewItemLoader(item=item, response=response)
    parsed_array = response.xpath(parse_xpath).getall()
    print('parsed_array', parsed_array)
    contacts_dict = defaultdict(dict)
    contacts_list = defaultdict(list)
    contact_header = None
    sanitize_string = list_to_string()
    
    for idx, parsed_line in enumerate(parsed_array):
      if 'img' not in parsed_line:
        if 'nav' in parsed_line:
          contact_header = sanitize_string(parsed_line)
        else:
          contact_header = contact_header or 'Contact Info'
          contacts_list[contact_header].append(parsed_line)
    
    for header_name in contacts_list:
      contact_loader = ContactItemLoader(item=ContactItem(), response=response)
      contact_array = contacts_list[header_name]
      for idx in range(0, len(contact_array), 2):
        field = sanitize_string(contact_array[idx]).lower()
        contact_loader.add_value(field, contact_array[idx + 1])
      contacts_dict[header_name].update(contact_loader.load_item())
    print('contacts_dict', contacts_dict)
    item_loader.add_value('contacts', contacts_dict)
    item_dict.update(item_loader.load_item())
    return item_dict
