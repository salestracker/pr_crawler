from absl import app
from absl import logging
import googlecloudprofiler

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pr_crawler.spiders.parse_directory import ParseDirectorySpider

def main(argv):
  # Profiler initialization. It starts a daemon thread which continuously
  # collects and uploads profiles. Best done as early as possible.
  try:
    googlecloudprofiler.start(
      service='pr-crawler-scrapy',
      service_version='1.0.0',
      # verbose is the logging level. 0-error, 1-warning, 2-info,
      # 3-debug. It defaults to 0 (error) if not set.
      verbose=3,
      # project_id must be set if not running on GCP.
      # project_id='my-project-id',
    )
    CrawlProcess = CrawlerProcess(get_project_settings())
    CrawlProcess.crawl(ParseDirectorySpider)
    CrawlProcess.start()
  except (ValueError, NotImplementedError) as exc:
    logging.error(exc)  # Handle errors here


if __name__ == "__main__":
  app.run(main)
