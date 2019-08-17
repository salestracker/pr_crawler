# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from scrapy.exporters import BaseItemExporter

from . import fire_db, encode_url

# Set log level to INFO.
logging.getLogger().setLevel(logging.INFO)


class FireStoreItemExporter(BaseItemExporter):
  '''Custom Feed Exporter based on Firestore.'''
  
  def __init__(self, collection_name):
    super().__init__()
    self.__collection_name = collection_name
    self.__collection = None
    self._exporter = None
    self.__firestore_batch_count = 0
    self.__batch = None
    self.__logger = logging.getLogger()

  @property
  def _batch_count(self):
    return self.__firestore_batch_count
  
  @_batch_count.setter
  def _batch_count(self, val):
    self.__firestore_batch_count += val
  
  @_batch_count.deleter
  def _batch_count(self):
    self.__firestore_batch_count = 0

  def _create_new_batch(self):
    self.__batch = fire_db.batch()

  def export_item(self, item_url):
    if not self._exporter:
      raise NotImplementedError
    if not self.__batch:
      raise NotImplementedError
    item, request_url = item_url
    if self._batch_count < 500:
      # Add to batch.
      url = encode_url(request_url)
      doc_ref = self._exporter.document(url)
      self.__batch.set(doc_ref, item)
      self._batch_count = 1
    else:
      # Commit batch
      self.__batch.commit()
      # Reset and start a new batch.
      self.__batch = None
      self._batch_count = 0
      self._create_new_batch()

  def serialize_field(self, field, name, value):
    serializer = field.get('serializer', lambda x: x)
    return serializer(value)

  def start_exporting(self):
    self._exporter = fire_db.collection(self.__collection_name)
    self._create_new_batch()

  def finish_exporting(self):
    self.__logger.info('Finishing FireStoreItemExporter')
    if self.__batch:
      self.__batch.commit()

  def process_export(self, item):
    company_overview_uri = item.pop('company_overview_uri', None)
    categories_uri = item.pop('categories_uri', None)
    contacts_uri = item.pop('contacts_uri', None)
  
    if company_overview_uri and item:
      self.export_item((item, company_overview_uri))
    categories_item = item.pop('categories', {})
    if categories_uri and categories_item:
      self.export_item(({'categories': categories_item}, categories_uri))
    contacts_item = item.pop('contacts', {})
    if contacts_uri and contacts_item:
      self.export_item(({'contacts': contacts_item}, contacts_uri))


class PrExportFirestorePipeline(object):
    '''Pipeline to Sync and export data to firestore.'''
    _collection_name = 'covered_links'
    
    def __init__(self):
      # Project ID is determined by the GCLOUD_PROJECT environment variable
      self.db_exporter = FireStoreItemExporter(self._collection_name)
    
    @classmethod
    def from_crawler(cls, _crawler):
      return cls()
    
    def open_spider(self, spider):
      spider.logger.info('Spider opened: %s', spider.name)
      self.db_exporter.start_exporting()
    
    def close_spider(self, spider):
      spider.logger.info('Spider closing: %s', spider.name)
      self.db_exporter.finish_exporting()
    
    def process_item(self, item, spider):
      self.db_exporter.process_export(item)
      return item


class PrCrawlerPipeline(object):

  def process_item(self, item, spider):
    return item
