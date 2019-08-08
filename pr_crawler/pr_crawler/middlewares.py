# -*- coding: utf-8 -*-

import logging
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import os
import random
import time

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

# Set log level to INFO.
logging.getLogger().setLevel(logging.INFO)


class PrCrawlSnoozeResumeMiddleware(RetryMiddleware):
  
  def __init__(self, settings):
    super().__init__(settings)
    self.__start_time = time.time()
  
  @property
  def _start_time(self):
    return self.__start_time
  
  @property
  def _sleep_diff(self):
    # Between 10 min and 1 hr.
    return int(os.getenv('SNOOZE_DIFF', random.randint(600, 3600)))
  
  @property
  def _sleep_time(self):
    # Between 1 hr and 2 hr.
    return int(os.getenv('SNOOZE_TIME', random.randint(3600, 7200)))
  
  @_start_time.setter
  def _start_time(self, new_time):
    self.__start_time = new_time
  
  def process_response(self, request, response, spider):
    if time.time() - self._start_time > self._sleep_diff:
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
  
  def process_spider_input(self, response, spider):
    # Called for each response that goes through the spider
    # middleware and into the spider.
    
    # Should return None or raise an exception.
    return None
  
  def process_spider_output(self, response, result, spider):
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
  def from_crawler(cls, crawler):
    # This method is used by Scrapy to create your spiders.
    s = cls()
    crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
    return s
  
  def process_request(self, request, spider):
    # Called for each request that goes through the downloader
    # middleware.
    
    # Must either:
    # - return None: continue processing this request
    # - or return a Response object
    # - or return a Request object
    # - or raise IgnoreRequest: process_exception() methods of
    #   installed downloader middleware will be called
    return None
  
  def process_response(self, request, response, spider):
    # Called with the response returned from the downloader.
    
    # Must either;
    # - return a Response object
    # - return a Request object
    # - or raise IgnoreRequest
    return response
  
  def process_exception(self, request, exception, spider):
    """
    # Called when a download handler or a process_request()
    # (from other downloader middleware) raises an exception.
    
    # Must either:
    # - return None: continue processing this exception
    # - return a Response object: stops process_exception() chain
    # - return a Request object: stops process_exception() chain
    """
    return None
  
  def spider_opened(self, spider):
    spider.logger.info('Spider opened: %s' % spider.name)
