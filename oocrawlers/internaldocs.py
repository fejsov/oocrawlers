'''
Scraper for Openoil internal docs
'''

from oocrawlers import basecrawlers


class InternalDocsCrawler(basecrawlers.DirectoryCrawler):
    DIR_ROOT='/data/internal_documents'
    URL_ROOT='https://internal-docs.openoil.net/docs/'
    LABEL = 'Openoil Internal Documents'
    SITE = 'https://internal-docs.openoil.net/docs'
