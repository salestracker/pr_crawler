from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pr_crawler.spiders.parse_directory import ParseDirectorySpider

if __name__ == "__main__":
  CrawlProcess = CrawlerProcess(get_project_settings())
  CrawlProcess.crawl(ParseDirectorySpider)
  CrawlProcess.start()
