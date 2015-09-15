import logging
from lxml import etree

from aleph.crawlers import Crawler, TagExists

RSS_FEED = 'http://www.rigzone.com/news/rss/rigzone_latest.aspx'
PAGE_URL = 'http://www.rigzone.com/news/article_pf.asp?a_id=%s'

log = logging.getLogger(__name__)


class RigZoneCrawler(Crawler):

    LABEL = "RigZone"
    SITE = "http://rigzone.com/"

    def crawl(self):
        logging.info('starting rigzone crawl')
        feed = etree.parse(RSS_FEED)
        url = feed.findtext('.//item/link')
        id = int(url.split('/a/', 1)[-1].split('/', 1)[0])
        for article_id in xrange(id, 1, -1):
            url = PAGE_URL % article_id
            try:
                id = self.check_tag(url=url)
                self.emit_url(url, package_id=id, article=True)
            except TagExists:
                pass
