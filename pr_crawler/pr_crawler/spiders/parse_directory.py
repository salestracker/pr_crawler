# -*- coding: utf-8 -*-
import glob
import os
import random
from collections import defaultdict
from urllib import parse as urlParse

from scrapy import Request, Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import CompanyOverviewItem, CompanyOverviewItemLoader, \
  ContactItem, ContactItemLoader, clean_contact_fields

_BASE_URI = 'https://www.pr.com'
_START_URI = r'https://www.pr.com/business-directory/'

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

_COMPANY_CONTACT_URI = ('//table[2]//table//table[1]//table//tr/td['
                        '2]//h3/a/@href[contains(., "contact")]')
_CONTACT_PARSE_XPATH = ('//table[2]//tr[1]/td/table//tr[2]/td[2]/table['
                        '3]//tr/td/table//tr[*]/td/table//tr/td')

_RAND_RANGE = (100, 1000)
_JOB_DIR = 'jobdir'
_RAND_DIR = '{job_dir!s}_{rand_num:<3d}'
# You can add many built in referenced settings here.
_CUSTOM_SETTINGS = {
  'JOBDIR': _RAND_DIR.format(
    # Add as many keys as are preformatted and available in Custom Settings.
    job_dir=_JOB_DIR,
    rand_num=random.randrange(*_RAND_RANGE)
  ).strip(),
}


def preset_jobdir(custom_settings_dict):
  '''Sanitize and set job directory before passing for crawl.
  
  params:
    custom_settings_dict: dict, Map of custom settings.
  returns:
    dict, Map of custom settings.

  '''
  cur_path = path = os.path.abspath(os.path.curdir)
  jobdir_path_rgx = os.path.join(cur_path, 'jobdir*')
  jobdir_fpath = glob.glob(jobdir_path_rgx)
  if jobdir_fpath and os.path.isdir(jobdir_fpath[0]):
    _, jobdir = os.path.split(jobdir_fpath[0])
    custom_settings_dict['JOBDIR'] = jobdir
  else:
    if not custom_settings_dict.get('JOBDIR', None):
      custom_settings_dict['JOBDIR'] = _RAND_DIR.format(
        # Add as many keys as are preformatted and available in Custom Settings.
        job_dir=_JOB_DIR,
        rand_num=random.randrange(*_RAND_RANGE)
      ).strip()
  return custom_settings_dict


class ParseDirectorySpider(CrawlSpider):
  """Spider class for PR site."""
  
  name = 'parse_directory'
  allowed_domains = ['pr.com']
  start_urls = [_START_URI]
  custom_settings = preset_jobdir(_CUSTOM_SETTINGS)
  
  rules = (
    Rule(
      LinkExtractor(allow=_BIZ_LINKS, deny=_DENY_LINKS),
      callback=None,
      follow=True),
    Rule(
      LinkExtractor(allow=_COMPANY_LINK, deny=_DENY_LINKS + _BIZ_LINKS),
      callback='parse_item',
      follow=False),
  )
  
  def parse_item(self, response):
    """Parse function for parsing company overview page of a company.
    
    params:
      response: object, A scrapy Response object.
    returns:
      object, a scrapy.Request object.
    """
    
    parsed_dict = {}
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
    href_dict = {
      'endpoint': contact_href,
      'parse_attributes': (contact_attributes, _CONTACT_PARSE_XPATH,
                           'parse_contact'),
    }
    uri = urlParse.urljoin(_BASE_URI, category_href)
    return self._yield_meta_request(uri, item_loader, _CATEGORIES_PARSE_XPATH,
                                    category_attributes,
                                    'parse_attribute', **href_dict)
  
  def parse_attribute(self, response):
    """Parse function for parsing category page of a company.
    
    params:
      response: object, A scrapy Response object.
    returns:
      object, a scrapy.Request object.
    """
    
    item_dict = response.meta['item_dict']
    selector = Selector(response)
    item_loader = CompanyOverviewItemLoader(item=item_dict,
                                            selector=selector,
                                            response=response)
    attributes = response.meta['attributes']
    parse_xpath = response.meta['parse_xpath']
    parsed_values = response.xpath(parse_xpath).getall()
    item_loader.add_value(attributes[0], parsed_values)
    href = response.meta['endpoint']
    href_val = response.meta['parse_attributes']
    uri = urlParse.urljoin(_BASE_URI, href)
    return self._yield_meta_request(uri, item_loader, href_val[1], href_val[0],
                                    href_val[2])
  
  def parse_contact(self, response):
    """Parse function for parsing contacts page.
    
    params:
      response: object, A scrapy Response object.
    returns:
      dict, a dictionary of parsed items.
    """
    
    item_dict = response.meta['item_dict']
    selector = Selector(response)
    item_loader = CompanyOverviewItemLoader(item=item_dict,
                                            selector=selector,
                                            response=response)
    parse_xpath = response.meta['parse_xpath']
    parsed_array = response.xpath(parse_xpath).getall()
    contacts_dict = defaultdict(dict)
    contacts_list = defaultdict(list)
    contact_header = None
    clean_field = clean_contact_fields()
    for idx, parsed_line in enumerate(parsed_array):
      if 'img' not in parsed_line:
        if 'nav' in parsed_line:
          contact_header = clean_field(parsed_line)
        else:
          contact_header = contact_header or 'Contact Info'
          contacts_list[contact_header].append(parsed_line)
    
    for contact_header in contacts_list:
      contact_loader = ContactItemLoader(
        item=ContactItem(), response=response)
      contact_array = contacts_list[contact_header]
      for idx in range(0, len(contact_array), 2):
        field = clean_field(contact_array[idx]).lower()
        contact_loader.add_value(field, contact_array[idx + 1])
      contacts_dict[contact_header].update(contact_loader.load_item())
    item_loader.add_value('contacts', contacts_dict)
    return item_loader.load_item()
  
  def _yield_meta_request(self, uri_endpoint, item_loader, xpath,
                          item_key, callback_fn, **kwargs):
    """Yields a request object based on meta object.
    
    params:
      uri_endpoint: str, the endpoint of with base uri to crawl.
      item_loader: object, Scrapy ItemLoader object.
      xpath: str, the XSLT xpath to crawl.
      item_key: str, the Item key to be added to Item loader.
      callback_fn: str, Function name to callback in Request object.
      kwargs: dict(optional), Keyword arguments to pass on to meta.

    yields:
       object, a scrapy.Request object.
    """
    
    uri = urlParse.urljoin(_BASE_URI, uri_endpoint)
    meta = {
      'item_dict': item_loader.load_item(),
      'attributes': item_key,
      'parse_xpath': xpath,
    }
    if kwargs:
      meta.update(kwargs)
    callback = getattr(self, callback_fn)
    yield Request(uri, callback=callback, meta=meta)
