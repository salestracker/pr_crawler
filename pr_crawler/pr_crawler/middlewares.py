# -*- coding: utf-8 -*-

import logging
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import os
import random
import time
import secrets

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.response import response_status_message

from . import FIRE_DB
from . import encode_url

# Set log level to INFO.
logging.getLogger().setLevel(logging.INFO)


# @firestore.transactional
def link_exists(request_url, collection):
  '''Check and skip already covered links.
  
  params:
    request_url: str, the requested url.
    collection: object, a firestore collection object.
    # transaction: object, a firestore transaction object.
  
  return:
    boolean, whether transaction succeeded or not.
  '''
  url = encode_url(request_url)
  doc_ref = collection.document(url)
  return doc_ref.get().exists


class SkipParsedUrlMiddleware(object):
  '''Skips Parsed Urls.'''

  _collection_name = 'covered_links'

  def __init__(self):
    super().__init__()
    self.__collection = FIRE_DB.collection(self._collection_name)

  def spider_opened(self, spider):
    spider.logger.info('Spider opened: %s' % spider.name)

  def process_request(self, request, spider):
    # URL being scraped
    url_exists = link_exists(request.url, self.__collection)
    if url_exists:
      spider.logger.info('Skipping URL. Already scrapped or in ' 'pipeline.')
      raise IgnoreRequest('Skipping URL. Already scrapped or in pipeline.')
    else:
      return None

  def process_exception(self, request, exception, spider):
    spider.logger.info('Skipping Request for url: %s with exception: %s',
                       request.url, exception)
    return None

  @classmethod
  def from_crawler(cls, crawler):
    # This method is used by Scrapy to create your spiders.
    create_spider = cls()
    crawler.signals.connect(create_spider.spider_opened,
                            signal=signals.spider_opened)
    return create_spider


class PrCrawlSnoozeResumeMiddleware(RetryMiddleware):
  '''Middleware that snoozes the crawler bot and resumes for sometime.'''

  def __init__(self, settings):
    super().__init__(settings)
    self.__random = secrets.SystemRandom(secrets.randbelow(600))
    self.__start_time = time.time()

  @property
  def _start_time(self):
    return self.__start_time

  @_start_time.setter
  def _start_time(self, new_time):
    self.__start_time = new_time

  @property
  def _running_time(self):
    # Between 10 min and 1 hr.
    choice = self.__random.choice(range(600, 3600))
    return int(os.getenv('RUNNING_TIME', choice))

  @property
  def _sleep_time(self):
    # Between 1 hr and 2 hr.
    choice = self.__random.choice(range(3600, 7200))
    return int(os.getenv('SNOOZE_TIME', choice))

  def process_response(self, request, response, spider):
    if time.time() - self._start_time > self._running_time:
      spider.logger.info('Spider going to sleep: %s' % spider.name)
      time.sleep(self._sleep_time)  # few minutes
      spider.logger.info('Spider woke up!: %s' % spider.name)
      self._start_time = time.time()
      reason = response_status_message(response.status)
      logging.info('wake up reason %s', reason)
      return self._retry(request, reason, spider) or response

    return super().process_response(request, response, spider)

  def spider_opened(self, spider):
    spider.logger.info('Spider opened: %s' % spider.name)


class PrCrawlerSpiderMiddleware(object):
  # Not all methods need to be defined. If a method is not defined,
  # scrapy acts as if the spider middleware does not modify the
  # passed objects.

  @classmethod
  def from_crawler(cls, crawler):
    # This method is used by Scrapy to create your spiders.
    s = cls()
    crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
    return s

  def process_spider_input(self, _response, _spider):  # pylint: disable=R0201
    # Called for each response that goes through the spider
    # middleware and into the spider.

    # Should return None or raise an exception.
    return None

  def process_spider_output(self, response, result, spider):  # pylint: disable=R0201
    # Called with the results returned from the Spider, after
    # it has processed the response.

    # Must return an iterable of Request, dict or Item objects.
    for i in result:
      yield i

  def process_spider_exception(self, response, exception, spider):
    # Called when a spider or process_spider_input() method
    # (from other spider middleware) raises an exception.

    # Should return either None or an iterable of Response, dict
    # or Item objects.
    pass

  def process_start_requests(self, start_requests, spider):
    # Called with the start requests of the spider, and works
    # similarly to the process_spider_output() method, except
    # that it doesn’t have a response associated.

    # Must return only requests (not items).
    for r in start_requests:
      yield r

  def spider_opened(self, spider):
    spider.logger.info('Spider opened: %s' % spider.name)


class PrCrawlerDownloaderMiddleware(object):
  # Not all methods need to be defined. If a method is not defined,
  # scrapy acts as if the downloader middleware does not modify the
  # passed objects.

  @classmethod
  def from_crawler(cls, crawler):  # pylint: disable=R0201
    # This method is used by Scrapy to create your spiders.
    create_spider = cls()
    crawler.signals.connect(create_spider.spider_opened,
                            signal=signals.spider_opened)
    return create_spider

  def process_request(self, request, spider):  # pylint: disable=R0201
    # Called for each request that goes through the downloader
    # middleware.

    # Must either:
    # - return None: continue processing this request
    # - or return a Response object
    # - or return a Request object
    # - or raise IgnoreRequest: process_exception() methods of
    #   installed downloader middleware will be called
    return None

  # pylint: disable=R0201
  def process_response(self, _request, response, _spider):
    # Called with the response returned from the downloader.

    # Must either;
    # - return a Response object
    # - return a Request object
    # - or raise IgnoreRequest
    return response

  # pylint: disable=R0201
  def process_exception(self, _request, _exception, _spider):
    """
    # Called when a download handler or a process_request()
    # (from other downloader middleware) raises an exception.
    
    # Must either:
    # - return None: continue processing this exception
    # - return a Response object: stops process_exception() chain
    # - return a Request object: stops process_exception() chain
    """
    return None

  def spider_opened(self, spider):  # pylint: disable=R0201
    spider.logger.info('Spider opened: %s' % spider.name)
