from oocrawlers.sedar import S3Crawler

class LSECrawler(S3Crawler):
    LABEL = 'LSE'
    BUCKET = 'data.openoil.net'
    PREFIX = 'lse'
    SITE = 'http://openoil.net'
    MAXITEMS = 999999999
    OFFSET = 0

    def collect_metadata(self, filename):
        meta = {}
        try:
            meta['companyCode'] = filename.split('/')[-2]
        except IndexError:
            pass
        return meta
