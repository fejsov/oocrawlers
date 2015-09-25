from aleph.crawlers import Crawler, TagExists
from BeautifulSoup import BeautifulSoup
import dateutil.parser
import json
import requests
import sys
import pprint


if __name__ == '__main__':
    from examplecrawler import TestCrawler
    Crawler = TestCrawler
        


COMPANIES_URL = 'http://sgx-api-lb-195267723.ap-southeast-1.elb.amazonaws.com/sgx/search?callback=jQuery1110005645173738973086_1442329596797&json=%7B%22criteria%22%3A%5B%5D%7D&_=1442329596800'
COMPANY_INFO_URL = 'http://www.sgx.com/wps/portal/sgxweb/home/company_disclosure/stockfacts?page=1&code={}'
ANNOUNCEMENTS_SEARCH_URL = 'http://www.sgx.com/proxy/SgxDominoHttpProxy'
INDUSTRIES = ['metals and minning', 'energy equipment and services', 'oil, gas and consumable fuels']
ANNOUNCEMENT_PERIODS = ['AnnouncementToday', 'AnnouncementLast3Months', 'AnnouncementLast6Months', 'AnnouncementLast12Months',
                        'AnnouncementLast1stYear', 'AnnouncementLast2ndYear', 'AnnouncementLast3rdYear', 'AnnouncementLast4thYear',
                        'AnnouncementLast5thYear']
ANNOUNCEMENT_URL = 'http://infopub.sgx.com/Apps?A=COW_CorpAnnouncement_Content&B={}&F={}'


class SingaporeCrawler(Crawler):
    LABEL = "Singapore"
    SITE = "http://www.sgx.com/"

    def list_companies(self):
        #===== Create empty list for storing basic companies meta data =====#
        companies = []
        
        #===== Create request for getting page with JSON objects containing companies data =====#
        response = requests.get(COMPANIES_URL)
        
        #===== Check if server returned page =====#
        if response.status_code == 200:
        
            #===== Try to convert JSON object to dictionary =====#
            try:
                data = eval(response.text[response.text.index('{'): response.text.rindex('}') + 1])
                
            #===== Some error occured, show error and exit =====#
            except Exception as error:
                print(error)
                sys.exit()
                
        #===== Server didn't return the page, show message and exit =====#
        else:
            print('Server didn\'t respond properly. Exiting program.')
            sys.exit()
            
        #===== Check if dictionary contains companies data =====#
        try:
        
            #===== Loop through the companies =====#
            for company in data['companies']:
            
                #===== Try to extract data for company =====#
                try:
                
                    #===== Get company name, industry and code =====#
                    company_name = company['companyName']
                    industry = company['industry']
                    code = company['tickerCode']
                    
                #===== Company doesn't have some of the fields that are required, skip this company ======#
                except KeyError:
                    continue
                  
                #===== If company is in required industries, add it to the companies list =====#  
                if industry.lower() in INDUSTRIES:
                    companies.append([company_name, {'code': code, 'industry': industry, 'company_info_url': COMPANY_INFO_URL.format(code)}])
        
        #===== Page doesn't contain companies data, show message and exit =====#            
        except KeyError:
            print('Page doesn\'t contain companies data. Exiting program.')
        
        #===== Return list with companies from required indestries =====#
        return companies

    def get_detailed_metadata(self, url):
        #===== Create variables that would contain wanted data =====#
        attachment_url = None
        detailed_metadata = {}
        
        #===== Create request for getting page for given URL =====#
        response = requests.get(url)
        
        #===== If page returned successfuly, create soup =====#
        if response.status_code == 200:
            soup = BeautifulSoup(response.text)
            
        #===== If page not retrieved correctly, return detailed metadata (which will be empty) =====#
        else:
            return [attachment_url, detailed_metadata]
            
        #===== Find tables with class 'table alertContainer' (those contains data we need) =====#
        tables = soup.findAll('table', {'class': 'table alertContainer'})
        
        #===== Loop through tables =====#
        for table in tables:
        
            #===== Find all rows in table =====#
            rows = table.findAll('tr')
            
            #===== Loop through the rows =====#
            for row in rows:
            
                #===== Find all columns in row (not exactly correct, as there are some td tags in lower tags, but it works) =====#
                columns = row.findAll('td')
                
                #===== If len(columns) < 2, it is header row, so skip it =====#
                if len(columns) < 2:
                    continue
                    
                #===== If Attachment is in left column, try to get URL =====#
                if 'Attachment' in columns[0].text:
                    try:
                        attachment_url = columns[1].find('a')['href']
                    except:
                        pass
                        
                #===== Some field different than Attachment, get name of field and value =====#
                else:
                    detailed_metadata[columns[0].text] = columns[1].text 
                    
        #===== Return obtained data =====#
        return [attachment_url, detailed_metadata]

    def announcements_for_company(self, company_name):
        #===== Create list for storing data =====#
        announcements = []
        
        #===== Set header fields for URL request =====#
        headers = {'Host': 'www.sgx.com',
                   'User-Agent': '"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:40.0) Gecko/20100101 Firefox/40.0"',
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Language': 'en-US,en;q=0.5',
                   'Accept-Encoding': 'gzip, deflate',
                   'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Referer': 'http://www.sgx.com/wps/portal/sgxweb/home/company_disclosure/company_announcements',
                   'Cookie': 'bbbbbbbbbbbbbbb=EIEPDLBFNKODKPLCAGIBLBCMDOFKHNABFLABJKHFONADMBCNNHICMIONLLMAJBGAKFJKGOJMOPMKKEHADEEFPCFNHDOHLPMGNEHCBBDJICODEMHOHKJEAAHHBAFEMEGA; _ga=GA1.2.2064021011.1441294878; JSESSIONID=0000MG8_tr5a1LoSWdyyJ1mnEXy:182j489ob; CokkiePDC=!AzjevSP6PxlfmdoxXwr0gW7Rp2anujjAHvC6EyaLTxIN/HfQTUX5uL0x9Y4sPngPkzQewheQbGsxcpI=; TS01e3a8b5=01d60216a7ca76a70ac57b08e8d88f65de63e8cd85ffa4bf4a07b76911f17c376a0335675b44a1c59335128522c4585f60893813d523300de60312e5706a6dfc68bf463e626cf019ad712c396afd354ac05892887face09cdd8c8f820f60930056018d63f4e0fc12e34e9cf9627488419a5c3b5b9ba8206a5d175b2ede96df2e77201bddf64b76458cafd9897ce068a781eb8ec6521fb82665729e03200ba2bdf9be514025c4156621bffa289527e8e33520a19a7616d3683c456c5585a3ada55532858c59d3bc40b5bfdeecb5aa7c38cea5c46d0f; CokkieSDC=!fu7W7fWm6ZNQ+3fnQIKRVj+WDDUzdfh6rS+jymrAB2qkQ7XFgEv9AwqgPtbgHMpLf3+11VXf3tZqxQ==; bbbbbbbbbbbbbbb=JKFCGFFELHBAMNEEPENCENHGGPJAODIHABIPHBGHGMADHBFBDLCEDKCBIDCAKAAFHNLKBPBIBADLPGEGFDNGEGLLCLLNNPBANODEEMGAFGPFEAGLMLFPKDAILOKGIOAE; __gads=ID=55934271166746c2:T=1442332068:S=ALNI_MYancNeo9c7QyY9A_DklUYNRovyeg; avr_1492338988_0_0_4294901760_2952962667_0=2211538107_27168815; avr_1492338988_0_0_4294901760_3053625963_0=2211538355_27168829; avr_1492338988_0_0_4294901760_3020071531_0=2211538351_27168829; avr_1492338988_0_0_4294901760_2986517099_0=2219381828_27168829; avr_1492338988_0_0_4294901760_3036848747_0=2211538359_27168829; avr_1492338988_0_0_4294901760_3003294315_0=2211541531_27169028',
                   'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache'}
                   
        #===== Loop through the announcement periods =====#
        for announcement_period in ANNOUNCEMENT_PERIODS:
        
            #===== Define parameters for URL request =====#
            params = {'timeout': '100',
                      'dominoHost': 'http://infofeed.sgx.com/Apps?A=COW_CorpAnnouncement_Content&B={}&R_C={}~ANNC~N_A&C_T=200'.format(announcement_period, company_name)}

            #===== Create request for getting announcement page =====#
            response = requests.post(ANNOUNCEMENTS_SEARCH_URL, headers=headers, params=params)
            
            #===== If page retrieved successfuly, process it =====#
            if response.status_code == 200:
            
                #===== Try to convert data to dictionary =====#
                try:
                
                    #===== If no announcements for given period, No Document Found will be in data, so move on next one =====#
                    if 'No Document Found' in response.text:
                        continue
                        
                    #===== Convert data to dictionary =====#
                    data = eval(response.text[response.text.index('['): response.text.index(']') + 1]) 
                    
                #===== Can't convert to dictionary, move to next announcement) =====#  
                except Exception as error:
                    print(error)
                    continue
                
                #===== Loop through announcements in dictionary ======#    
                for announcement in data:
                
                    #===== Try to find key (part of URL to be added to predefined URL) =====#
                    try:
                        key = announcement['key']
                        
                    #===== No key field in current annuncement, move to next =====#
                    except KeyError:
                        continue
                        
                    #===== Create announcement URL and append to announcements list =====#
                    announcement = ANNOUNCEMENT_URL.format(announcement_period, key)
                    announcements.append([announcement, {}])
        
        #===== Return announcements list =====#
        return announcements

    def massage_metadata(self, metadata):
        '''
        Adapt metadata to conform to standard field names
        '''
        metadata['Company Name'] = metadata.get(u'Submitted By (Co./ Ind. Name)', '')
        metadata['Company ID'] = metadata.get('code', '')
        metadata['Industry Sector'] = metadata.get('industry', '')
        try:
            metadata['Filing Date'] = dateutil.parser.parse(metadata['Date of receipt of notice by Listed Issuer']).strftime('%F')
        except KeyError:
            try:
                metadata['Filing Date'] = dateutil.parser.parse(metadata[u'Date &amp; Time of Broadcast']).strftime('%F')
            except KeyError:
                pass
        metadata['Announcement Title'] = metadata.get('Announcement Title',
                                                      metadata.get('title',
                                                                   ''))
        return metadata
    
    def crawl(self):
        for company_name, basic_company_data in self.list_companies():
            for (announcement_url, announcement_metadata) in self.announcements_for_company(company_name):
                attachment_url, detailed_metadata = self.get_detailed_metadata(announcement_url)
                detailed_metadata.update(announcement_metadata)
                detailed_metadata.update(basic_company_data)
                detailed_metadata = self.massage_metadata(detailed_metadata)
                try:
                    # Here we check that our datastore does not already
                    # contain a document with this URL
                    # Doing so enables us to re-run the scraper without
                    # filling the datastore with duplicates
                    
                    id = self.check_tag(url=attachment_url)

                    # This is the line that triggers the import into our system
                    # Aleph will then download the url, store a copy,
                    # extract the text from it (doing OCR etc as needed)
                    # and index text, title and metadata
                    self.emit_url(
                        url = attachment_url,
                        title = detailed_metadata['Announcement Title'],
                        meta = detailed_metadata,
                    )

                except TagExists:
                    pass


if __name__ == '__main__':
    ec = SingaporeCrawler('test')
    ec.crawl()
