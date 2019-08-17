# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from google.cloud import firestore
from scrapy.exporters import BaseItemExporter
from . import fire_db


class FireStoreItemExporter(BaseItemExporter):
  '''Custom Feed Exporter based on Firestore.'''
  
  def __init__(self, collection_name):
    super().__init__()
    self.__collection_name = collection_name
    self.__collection = None
    self._exporter = None
    self.__firestore_batch_count = 0
    self.__batch = None

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
    if not self._exporter or not self.__batch:
      raise NotImplementedError
    item, request_url = item_url
    if self._batch_count < 500:
      # Add to batch.
      doc_ref = self._exporter.document(request_url)
      self.__batch.set(doc_ref, item)
      self._batch_count = 1
    else:
      # Commit batch
      self.__batch.commit()
      # Reset and start a new batch.
      self._batch_count = 0
      self._create_new_batch()

  def serialize_field(self, field, name, value):
    serializer = field.get('serializer', lambda x: x)
    return serializer(value)

  def start_exporting(self):
    self._exporter = fire_db.collection(self._collection_name)
    self._create_new_batch()

  def finish_exporting(self):
    pass
  
  def process_export(self, item):
    company_overview_uri = item.pop('company_overview_uri', {})
    categories_uri = item.pop('categories_uri', {})
    contacts_uri = item.pop('contacts_uri', {})
    
    self.export_item((item, company_overview_uri))
    categories_item = item.pop('categories', {})
    self.export_item((categories_item, categories_uri))
    contacts_item = item.pop('contacts', {})
    self.export_item((contacts_item, contacts_uri))


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
      spider.logger.info('Spider opened: %s' % spider.name)
    
    def close_spider(self, spider):
      spider.logger.info('Spider closing: %s' % spider.name)
    
    def process_item(self, item, spider):
      self.db_exporter.process_export(item)
      return item


class PrCrawlerPipeline(object):

  def process_item(self, item, spider):
    return item
