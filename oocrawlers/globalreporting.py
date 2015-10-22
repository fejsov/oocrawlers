from aleph.crawlers import Crawler, TagExists
from BeautifulSoup import BeautifulSoup as BS
import json
from random import randint
import re
import requests
import sys
from time import sleep


BASE_URL = 'http://database.globalreporting.org'
SEARCH_URL = 'http://database.globalreporting.org/search/reports'
INDUSTRIES = {'Minning': 24,
              'Energy': 12}
REPORTS_URL = 'http://database.globalreporting.org/reports/view/{}'
DEBUG = True
PROXIES = {}
#PROXIES = {'http': 'http://213.85.92.10:80'}


class GlobalReportingCrawler(Crawler):
    LABEL = "Global Reporting"
    SITE = "https://www.globalreporting.org"

    def list_companies(self):
        #===== Create empty list for storing basic companies meta data =====#
        companies = []
        
        #===== Create session =====#
        session = requests.Session()
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'database.globalreporting.org',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0',
                   'Accept': 'text/html, */*; q=0.01',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': 'http://database.globalreporting.org/search',
                   'Cookie': 'CAKEPHP=md3fbpp0ub7c5bof46a5deq0p0; _ga=GA1.2.1166542630.1445514427; _gat=1; CakeCookie[LastSearch][r_c_sector]=Q2FrZQ%3D%3D.skQ%3D; __atuvc=1%7C42; __atuvs=5628ccf9f0aaa0cc000; __utma=128762842.1166542630.1445514427.1445514667.1445514667.1; __utmb=128762842.3.10.1445514667; __utmc=128762842; __utmz=128762842.1445514667.1.1.utmcsr=database.globalreporting.org|utmccn=(referral)|utmcmd=referral|utmcct=/search; __utmt=1; __unam=2ae556f-1508f640340-42c4eb08-1',
                   'Connection': 'keep-alive',
                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   }
        
        #===== Loop through all possible parameters =====#
        for industry, industry_value in INDUSTRIES.items():
        
            #===== Counter for pages and results per page value =====#
            page = 0
            results_per_page = 500
            
            while True:
                          
                #===== Define parameters for URL request =====#
                data = {'data[Sector][name]': '{}'.format(industry_value),
                        'data[Filter][init]': '{}'.format(page * results_per_page),
                        'data[Filter][results]': '{}'.format(results_per_page),
                        }
                
                #===== Create request for getting page with list of companies for given search query =====#
                try:
                    response = session.post(SEARCH_URL, headers=headers, data=data, proxies=PROXIES)
                except Exception as error:
                    print(error)
                    sys.exit()
                
                #===== Server returned page =====#
                if response.status_code == 200:
                    
                    #===== Create soup =====#
                    soup = BS(response.text)
                    
                    #===== Find element containing data (ul with list of li elements) =====#
                    data_table = soup.find('ul', {'id': 'list-of-reports'})              
                    
                    #===== Try to find tables for each company if data_table exists =====#
                    if data_table:
                        rows = data_table.findAll('table', {'class': 'report-details-table'})
                        
                        #===== If no rows, last page exceeded, break while loop =====#
                        if not rows:
                            break
                        
                        #===== Loop through rows =====#
                        for row in rows:
                            
                            #===== Find columns with data =====#
                            columns = row.findAll('td')
                            
                            #===== Try to get data =====#
                            try:
                            
                                #===== Get company name and create dictionary for basic company data =====#
                                company_name = columns[0].findAll('a')[0].text.encode('utf-8')
                                if DEBUG:
                                    print('Scraping data for {} in industry {}'.format(company_name, industry))
                                basic_company_data = {}
                                
                                #===== Get data =====#
                                basic_company_data['industry'] = industry
                                basic_company_data['company_url'] = BASE_URL + columns[0].findAll('a')[0]['href']                                
                                
                            #===== Some error, skip entry =====#
                            except Exception as error:
                                if DEBUG:
                                    print(industry)
                                    print(error)
                                continue
                                
                            #===== Add data to the companies list =====#
                            companies.append([company_name, basic_company_data])
                            
                    #===== Add some sleep to avoid blocking by server =====#
                    sleep(randint(1, 20) / 10.0)
                    
                    #===== Inrease page number =====#
                    page += 1
                    
                #===== Server didn't return the page, show message =====#
                else:
                    print('Error getting page for industry {}'.format(industry))
                
        #===== Remove duplicates (companies are retrieved from reports page, it might be several reports from same companies =====#
        unique_companies = []
        for company in companies:
            if company not in unique_companies:
                unique_companies.append(company)
        companies = unique_companies
            
        print('Found {} companies.'.format(len(companies)))
         
        #===== Return list of company URLs with basic metadata =====#
        return companies
        
    def get_detailed_metadata(self, company_url):
    
        #===== Create empty soup (in case no soup can be created from page) =====#
        soup = BS('')
    
        #===== Create variables that would contain wanted data =====#
        detailed_metadata = {}
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'database.globalreporting.org',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': 'http://database.globalreporting.org/search',
                   'Cookie': 'CAKEPHP=md3fbpp0ub7c5bof46a5deq0p0; _ga=GA1.2.1166542630.1445514427; _gat=1; CakeCookie[LastSearch][r_c_sector]=Q2FrZQ%3D%3D.skQ%3D; __atuvc=1%7C42; __atuvs=5628ccf9f0aaa0cc000; __utma=128762842.1166542630.1445514427.1445514667.1445514667.1; __utmb=128762842.3.10.1445514667; __utmc=128762842; __utmz=128762842.1445514667.1.1.utmcsr=database.globalreporting.org|utmccn=(referral)|utmcmd=referral|utmcct=/search; __utmt=1; __unam=2ae556f-1508f640340-42c4eb08-1',
                   'Connection': 'keep-alive',
                   }
        
        #===== Create request for getting company page =====#
        try:
            response = requests.get(company_url, headers=headers, proxies=PROXIES)
            
            #===== If page returned successfuly, get details =====#
            if response.status_code == 200:
            
                #===== Create soup =====#
                soup = BS(response.text)
                
                #===== Get description =====#
                detailed_metadata['description'] = soup.find('p', {'id': 'c-description'}).text.encode('utf-8')
                
                #===== Find li elements containing information =====#
                rows = soup.find('div', {'id': 'top-bottom'}).findAll('li')
                
                #===== Loop through li elements =====#
                for row in rows:
                
                    #====== Try to get data =====#
                    try:
                    
                        #===== Find description and value fields and update detailed metadata =====#
                        description = row.find('span', {'class': 'data-desc'}).text.encode('utf-8')
                        value = row.find('span', {'class': re.compile('data-value')}).text.encode('utf-8')
                        detailed_metadata[description] = value
                        
                    #===== In case value is not defined, skip this entry =====#
                    except Exception as error:
                        print(error)
                    
            #===== Server didn't return the page, show message =====#
            else:
                print('Server didn\'t returned page for {}'.format(company_url))
                
        except Exception as error:
            print(error)
            print('Error URL: {}'.format(company_url))
                    
        #===== Return obtained data =====#
        return detailed_metadata, soup

    def announcements_for_company(self, soup):
    
        #===== Create list for storing annoumcements IDs found on the company page =====#
        announcements_ids = [announcement_id['id'].strip('r') for announcement_id in soup.findAll('img', {'class': 'item-report'})]
        
        #===== Create list for storing data =====#
        announcements = []
        
        #===== Create session =====#
        session = requests.Session()
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'database.globalreporting.org',
                   'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Referer': 'http://database.globalreporting.org/search',
                   'Cookie': 'CAKEPHP=md3fbpp0ub7c5bof46a5deq0p0; _ga=GA1.2.1166542630.1445514427; _gat=1; CakeCookie[LastSearch][r_c_sector]=Q2FrZQ%3D%3D.skQ%3D; __atuvc=1%7C42; __atuvs=5628ccf9f0aaa0cc000; __utma=128762842.1166542630.1445514427.1445514667.1445514667.1; __utmb=128762842.3.10.1445514667; __utmc=128762842; __utmz=128762842.1445514667.1.1.utmcsr=database.globalreporting.org|utmccn=(referral)|utmcmd=referral|utmcct=/search; __utmt=1; __unam=2ae556f-1508f640340-42c4eb08-1',
                   'Connection': 'keep-alive',
                   }
                   
        #===== Loop until no Next >> on the page =====#
        for announcement_id in announcements_ids:
        
            #===== Create dict for storing announcement metadata =====#
            announcement_metadata = {}
        
            #===== Create URL for announcement =====#
            url = REPORTS_URL.format(announcement_id)
        
            #===== Create request for getting announcement page =====#
            try:
                response = session.get(url, headers=headers, proxies=PROXIES)
            except:
                continue
            
            #===== If page retrieved successfuly, process it =====#
            if response.status_code == 200:
            
                if DEBUG:
                    print('Scraping data from {}'.format(url))
                    
                #===== Create soup =====#
                soup = BS(response.text)
                
                #===== Find announcement title =====#
                announcement_metadata['title'] = soup.find('h3', {'class': 'report-title'}).text.encode('utf-8')
                
                #===== Try to find announcement PDF url =====#
                try:
                    announcements_url = soup.find('span', {'id': 'c-pdf_url'}).a['href']
                except:
                    
                    #===== Try to find announcement HTML url if PDF url doesn't exist =====#
                    try:
                        announcements_url = soup.find('span', {'id': 'c-html_url'}).a['href']
                    except:
                        announcements_url = None
                        
                #===== If neither of URLs is found go to next announcement =====#
                if not announcements_url:
                    continue
                    
                #===== Find tables with data =====#
                tables = soup.findAll('table', {'class': 'datasheet-table'})
                
                #===== Loop through the tables =====#
                for table in tables:
                
                    #===== Find rows with data in table =====#
                    rows = table.findAll('tr')
                    
                    #===== Loop through the rows =====#
                    for row in rows:
                    
                        #===== Get description and value fields and add to announcement metadata dict =====#
                        description = row.find('td', {'class': 'data-label'}).text.encode('utf-8').strip(':')
                        value = row.find('td', {'class': 'data-value'}).text.encode('utf-8')
                        if 'positive-value' in str(row.find('td', {'class': 'data-value'})):
                            value = 'yes'
                        elif 'negative-value' in str(row.find('td', {'class': 'data-value'})):
                            value = 'no'
                        announcement_metadata[description] = value
                    
                #===== Append data to announcements list =====#
                announcements.append([announcements_url, announcement_metadata])
                    
            #===== If not, show error page =====#
            else:
                print(response.text)
                continue
        
        #===== Return announcements list =====#
        return announcements
    
    def crawl(self):
        for company_name, basic_company_data in self.list_companies(): 
            detailed_metadata, soup = self.get_detailed_metadata(basic_company_data['company_url'])
            detailed_metadata.update(basic_company_data)
            for announcement_url, announcement_metadata in self.announcements_for_company(soup):
                detailed_metadata.update(announcement_metadata)

                if DEBUG:
                    print(announcement_url, detailed_metadata)
                
                try:
                    # Here we check that our datastore does not already
                    # contain a document with this URL
                    # Doing so enables us to re-run the scraper without
                    # filling the datastore with duplicates
                    
                    id = self.check_tag(url=announcement_url)

                    # This is the line that triggers the import into our system
                    # Aleph will then download the url, store a copy,
                    # extract the text from it (doing OCR etc as needed)
                    # and index text, title and metadata
                    self.emit_url(
                        url = announcement_url,
                        title = announcement_metadata['title'],
                        meta = detailed_metadata,
                    )

                except TagExists:
                    pass
        
                    
grc = GlobalReportingCrawler('test')
grc.crawl()
