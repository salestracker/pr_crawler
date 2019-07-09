# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import CompanyOverviewItem, CompanyOverviewItemLoader

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
]

# XPATHS
# _START_XPATH = ('//table/descendant::form/descendant::table/descendant
#                 '::table/descendant::table/descendant::span/descendant::b
#                 /a[@href]')

_COMPANY_DESC_XPATH = ('//table[2]/descendant::table[contains(name(), '
                       'tbody)]/descendant::div/div[2]/text()')
_COMPANY_PROFILE_IMG = ('//table[2]/descendant::table/descendant::table[1]'
                        '[contains(name(),tr[3])][contains(name(), '
                        'h1)]//img/@src[starts-with(., "https")]|//table//tr['
                        '2]//img/@src[starts-with(., "https")]')
_COMPANY_NAME_XPATH = '//table[2]//table//table[1]//tr[3]//h1/text()'

_COMPANY_FIELDS = {
  'common': r'//table[3]//table',
  'status': r'//tr[1]/td[2]/text()',
  'founded': r'//tr[1]/td[4]/text()',
  'annual_revenue': r'//tr[1]/td[6]/text()',
  'exchange': r'//tr[2]/td[2]/text()',
  'pr_link_id': r'//tr[2]/td[4]/text()',
  'employees': r'//tr[2]/td[6]/text()',
  'symbol': r'//tr[3]/td[2]/text()',
  'website': r'//tr[3]/td[4]/text()',
}

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
    item_loader.add_xpath('company_description', _COMPANY_DESC_XPATH)
    item_loader.add_xpath('img', _COMPANY_PROFILE_IMG)
    item_loader.add_xpath('name', _COMPANY_NAME_XPATH)
    field_loader = item_loader.nested_xpath(_COMPANY_FIELDS['common'])
    for key in _COMPANY_FIELDS:
      field_loader.add_xpath(key, _COMPANY_FIELDS[key])
    return item_loader.load_item()
