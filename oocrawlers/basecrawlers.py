'''
Utility crawlers for common purposes
'''

import os
import re
import logging

from aleph.crawlers import Crawler, TagExists
log = logging.getLogger(__name__)


class DirectoryCrawler(Crawler):
    '''
    Crawl a local directory which is also available online
    Subclass this, set DIR_ROOT and URL_ROOT, and you're set

    NB you will also need to set up a server to get the directory online
    with nginx, you're looking at something like:

	location /EITI {
		 root /data;		 
		 }

    '''

    DIR_ROOT = ''
    URL_ROOT = ''

    def title_from_filepath(self, filepath):
        fn = filepath.split('/')[-1]
        fn = re.sub('[-_]+', ' ', fn).title()
        return fn

    def collect_metadata(self, filepath, url):
        return {
            'filepath': filepath,
            'title': self.title_from_filepath(filepath),
        }
    
    def crawl(self):
        log.info('starting crawl')
        for (filepath, url) in self.iterate_dir():
            self.emit_url(
                url,
                meta = self.collect_metadata(filepath, url)
                )

    def iterate_dir(self):
        log.info('iterating dir')
        for (subdir, dirs, files) in os.walk(self.DIR_ROOT):
            for file in files:
                log.info('building path for %s' % file)
                filepath = os.path.join(subdir, file)
                url = filepath.replace(self.DIR_ROOT, self.URL_ROOT, 1)
                yield (filepath, url)

                


