'''
Scraper for Openoil internal docs
'''

from oocrawlers import basecrawlers


class EITICrawler(basecrawlers.DirectoryCrawler):
    DIR_ROOT='/data/EITI'
    URL_ROOT='https://internal-docs.openoil.net/EITI/'
    LABEL = 'EITI Reports'
    SITE = 'https://internal-docs.openoil.net/EITI'
