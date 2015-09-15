import json
import logging
import re
import requests
import subprocess
from lxml import etree
from urlparse import urljoin
from werkzeug.utils import secure_filename
import six

import dataset
import Levenshtein

from aleph.crawlers import Crawler, TagExists



log = logging.getLogger(__name__)

NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'
BUCKET = 'https://s3-eu-west-1.amazonaws.com/downloads.openoil.net/?prefix=contracts/'


# dataset

dburl='postgresql://openoil:EJLENtQZ2Lp766wB9tD8@127.0.0.1/sedar'
engine = dataset.connect(dburl)


class S3Crawler(Crawler):

    LABEL = "AWS"
    MAXITEMS = 20000
    OFFSET = 0

    def crawl(self):
        self.crawlbucket(
            bucket = self.BUCKET,
            prefix = self.PREFIX,
            maxitems = self.MAXITEMS + self.OFFSET)
            
    def collect_metadata(self, filename):
        meta = {}
        if 'oil/' in filename:
            meta['industry'] = 'oil'
        else:
            meta['industry'] = 'unknown'
        if 'material_document' in filename:
            meta['document_type'] = 'material document'
        year_co = re.search('(\d{4})/(\d{6,9})/\d+/', filename)
        if year_co:
            meta['date'] = year_co.group(1)
            meta['sedar_company_id'] = year_co.group(2)
        return meta

    def crawlbucket(self, bucket, prefix, maxitems=2000):
        cmd = "aws s3api list-objects --bucket %s --prefix %s --max-items %s --query 'Contents[].{Key:Key}'" % (bucket, prefix, maxitems)
        logging.warn('running %s' % cmd)
        keylist = json.loads(subprocess.check_output(cmd, shell=True))
        for d in keylist[self.OFFSET:]:
            logging.warn('working on %s' % d['Key'])
            url ='http://%s.s3.amazonaws.com/%s' % (bucket, d['Key'])
            meta = self.collect_metadata(d['Key'])

            try:
                self.check_tag(url=url)
                self.emit_url(
                    url,
                    meta = meta)
            except TagExists:
                logging.info('skipping existing file %s' % url)
                pass

            
class SedarCrawler(S3Crawler):
    LABEL = 'SEDAR'
    BUCKET = 'sedar.openoil.net'
    PREFIX = 'mining_material_'
    SITE = 'http://openoil.net'
    MAXITEMS = 100000
    #OFFSET = 6970
    OFFSET=0 # XXX we should go back and check the 

    def collect_metadata(self, filename):
        return {}
        meta = {}
        if 'oil/' in filename:
            meta['industry'] = 'oil'
        elif 'mining_material' in filename:
            meta['industry'] = 'mining'
        else:
            meta['industry'] = 'unknown'
        if 'material_document' in filename:
            meta['document_type_basic'] = 'material document'
        year_co = re.search('(\d{4})/(\d{6,9})/\d+/', filename)
        if year_co:
            meta['date'] = year_co.group(1)
            meta['sedar_company_id'] = year_co.group(2)

        dbrow = db_best_filename_match(filename)
        if dbrow:
            for dbname, esname in (
                    ('date', 'date'),
                    ('format', 'format'),
                    ('document_type', 'type'),
                    ('filesize', 'size'),
                    ('industry', 'industry'),
                    ('publication_time', 'time'),
                    ('source_url', 'tos_form'),
            ):
                if esname in dbrow:
                    meta[dbname] = dbrow[esname]
            logging.debug('added db metadata for %s' % filename)
        return meta

    


def build_mining_comp_list():
    fn = '/data/openoil_old/mining/companies_by_sic.txt'
    compdict = {}
    for row in open(fn):
        parts = row.split('\t')
        compdict[parts[0]] = parts[1]
    return compdict

EDGAR_COMPANIES = build_mining_comp_list()

    
class OOEdgarCrawler(S3Crawler):
    '''
    ingest the data we already have from edgar
    To be replaced with pudo's standard version, when
    we can wait a week to download everything
    '''
    LABEL = 'EDGAR Mining (extracted text only)'
    BUCKET = 'sec-edgar.openoil.net'
    SECTOR = 'oil'
    #PREFIX = 'oil/edgar_filings_text_by_company/'
    PREFIX = 'oil/edgar_filings_text_by_company/1000229'
    SITE = 'http://openoil.net'
    MAXITEMS=2000000
    
    def collect_metadata(self, filepath):
        # XXX writeme
        #filepath = 'mining/edgar_filings_text/944893/13D_1995-05-05_0000912057-95-003174.txt_extracted_0.txt'
        metadata = {
            'industry': filepath.split('/')[0],
            'sector': self.SECTOR,
            'company_id': filepath.split('/')[2],
            }
        if metadata['company_id'] in EDGAR_COMPANIES:
            metadata['company_name'] = EDGAR_COMPANIES[metadata['company_id']]
        filename = filepath.split('/')[-1]
        exhibit_num = re.search('(\d+).txt$', filepath).group(1)
        metadata['exhibit_number'] = str(int(exhibit_num) + 1)
        metadata['filing_type'], metadata['date'] = filename.split('_')[:2]

        filenum = re.search('_([\d\-]+).txt_extracted_', filepath).group(1)
        filenum_short = ''.join(filenum.split('-'))
        metadata['source_url']= 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm' % (metadata['company_id'], filenum_short, filenum)
        return metadata


def db_best_filename_match(url):
    '''
    from the filename, find the (most likely) corresponding item in the db

    there's a lot of faff here -- to make sense of it, look at the processes
    in scrape.py in the sedar repo, which is what we are reversing
    '''
    
    fnregex = '([^/]*/[^/]*/)([^/]*$)'
    filingdir, filename = re.search(fnregex, url).groups() # no error handling
    like_filingdir = '%%%s%%' % filingdir
    docs_in_dir = engine.query("select * from filing_index where filing like (:like_filingdir)", like_filingdir=like_filingdir)

    def fn_distance(dbrow):
        rawfn = dbrow['filing'].split('/')[-1]
        fnversion = secure_filename(
            # this is to match the manipultion in scrape.py in the sedar repo
            six.moves.urllib.parse.unquote(rawfn))
        return Levenshtein.distance(str(fnversion), str(filename))
    try:
        return min(docs_in_dir, key=fn_distance)
    except ValueError: #empty list -- no match in db
        logging.warning('could not find match for %s in db' % url)
        return None

    

def test_sedar_metadata():
    sampleurl = 'http://sedar.openoil.net.s3.amazonaws.com/oil/oil_material_documents_2013/02147049/00000001/kCorpSedarTmpSGPDonnycreek2013FilingsUnderwritingAgreement.pdf'
    return db_best_filename_match(sampleurl)
