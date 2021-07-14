# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Download Links Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import warnings
import logging
import regex as re

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MOD_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MOD_DIR, os.pardir))
RES_DIR = os.path.join(ROOT_DIR, 'resources')
SAVE_DIR = os.path.join(ROOT_DIR, 'save')
REPO_DIR = os.path.join(SAVE_DIR, 'greatschools')
DRIVER_FILE = os.path.join(RES_DIR, 'chromedriver.exe')
QUEUE_FILE = os.path.join(RES_DIR, 'zipcodes.zip.csv')
REPORT_FILE = os.path.join(SAVE_DIR, 'greatschools', 'links.csv')
if not ROOT_DIR in sys.path: sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webapi import WebURL, WebCache, WebQueue, WebDownloader
from webscraping.webdrivers import WebDriver
from webscraping.webtimers import WebDelayer
from webscraping.webpages import WebBrowserPage, CaptchaError, BadRequestError, MulliganError, WebContents
from webscraping.webactions import WebMoveToClick
from webscraping.webdata import WebClickable, WebClickableList, WebClickableDict, WebText, WebLink, WebBadRequest, WebCaptcha
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['Greatschools_Links_WebDelayer', 'Greatschools_Links_WebDownloader', 'Greatschools_Links_WebQueue']
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


captcha_xpath = r"//*[contains(@class, 'Captcha') or contains(@id, 'Captcha')]"
badrequest_xpath = r"//span[@class='heading' and contains(text(), 'Your search did not return any schools')]"
zipcode_xpath = r"//div[@class='pagination-summary']"
results_xpath = r"//div[@class='pagination-summary']"
contents_xpath = r"//section[@class='school-list']//ol/li[not(contains(@class, 'ad'))]"
link_contents_xpath = r".//a[@class='name']"
address_contents_xpath = r".//div[@class='address']"
name_contents_xpath = r".//a[@class='name']"
type_contents_xpath = r".//div/span[contains(@class, 'open-sans')]"
previous_xpath = r"//div[@class='pagination-container']//a[.//span[contains(@class, 'chevron-right rotate')] and not(contains(@class, 'disabled'))]"
nextpage_xpath = r"//div[@class='pagination-container']//a[.//span[@class='icon-chevron-right'] and not(contains(@class, 'disabled'))]"
current_xpath = r"//div[@class='pagination-container']//a[contains(@class, 'active')]"
pagination_xpath = r"//div[@class='pagination-container']//a[not(.//span)]"


def GID_parser(string): return string.replace("https://www.greatschools.org", "")

zipcode_parser = lambda x: str(re.findall(r"\d{5}$", x)[0])
results_parser = lambda x: str(re.findall(r"(?<=of )[\d\,]+(?= schools)", x)[0])
address_parser = lambda x: Address.fromsearch(x)
name_parser = lambda x: str(x).strip()
type_parser = lambda x: str(re.findall(re.compile(r"Public district|Public charter|Private", re.IGNORECASE), x)[0])
pagination_parser = lambda x: str(int(str(x).strip()))


class Greatschools_Captcha(WebCaptcha, xpath=captcha_xpath, timeout=2): pass
class Greatschools_BadRequest(WebBadRequest, xpath=badrequest_xpath, timeout=2): pass
class Greatschools_Zipcode(WebText.update(dataparser=zipcode_parser), xpath=zipcode_xpath): pass
class Greatschools_Results(WebText.update(dataparser=results_parser), xpath=results_xpath): pass
class Greatschools_Contents(WebClickableList, xpath=contents_xpath): pass
class Greatschools_ContentsLink(WebLink.update(dataparser=GID_parser), xpath=link_contents_xpath, parent=Greatschools_Contents, key='link'): pass
class Greatschools_ContentsAddress(WebText.update(dataparser=address_parser, optional=True), xpath=address_contents_xpath, parent=Greatschools_Contents, key='address'): pass
class Greatschools_ContentsName(WebText.update(dataparser=name_parser, optional=True), xpath=name_contents_xpath, parent=Greatschools_Contents, key='name'): pass
class Greatschools_ContentsType(WebText.update(dataparser=type_parser, optional=True), xpath=type_contents_xpath, parent=Greatschools_Contents, key='type'): pass
class Greatschools_Current(WebText.update(dataparser=pagination_parser), xpath=current_xpath): pass
class Greatschools_Previous(WebClickable, xpath=previous_xpath): pass
class Greatschools_Next(WebClickable, xpath=nextpage_xpath): pass
class Greatschools_Pagination(WebClickableDict.update(keyparser=pagination_parser), xpath=pagination_xpath): pass

class Greatschools_Previous_MoveToClick(WebMoveToClick, on=Greatschools_Previous): pass
class Greatschools_Next_MoveToClick(WebMoveToClick, on=Greatschools_Next): pass
class Greatschools_Pagination_MoveToClick(WebMoveToClick, on=Greatschools_Pagination): pass

class Greatschools_Links_WebDelayer(WebDelayer): pass 
class Greatschools_Links_WebDriver(WebDriver, options={'headless':False, 'images':True, 'incognito':False}): pass
class Greatschools_Links_WebURL(WebURL, protocol='https', domain='www.greatschools.org', datasets={}, separator='%20', spaceproxy='-'):
    def path(self, *args, **kwargs): 
        if 'zipcode' in kwargs.keys(): return ['search', 'search.zipcode']
        elif 'city' in kwargs.keys() and 'state' in kwargs.keys(): ['search', 'search.page']
        else: raise ValueError(list(kwargs.keys()))
        
    def parms(self, *args, **kwargs):
        if 'zipcode' in kwargs.keys(): return {'zip':'{:05.0f}'.format(int(kwargs['zipcode']))}
        else: return {'q':self.separator.join([item.replace(" ", self.spaceproxy).lower() for item in (kwargs['city'], kwargs['state'])])}


class Greatschools_Links_WebContents(WebContents):
    ZIPCODE = Greatschools_Zipcode
    RESULTS = Greatschools_Results
     
Greatschools_Links_WebContents.ITERATOR += Greatschools_Contents
Greatschools_Links_WebContents.PREVIOUS += Greatschools_Previous_MoveToClick
Greatschools_Links_WebContents.NEXT += Greatschools_Next_MoveToClick
Greatschools_Links_WebContents.CURRENT += Greatschools_Current
Greatschools_Links_WebContents.PAGINATION += Greatschools_Pagination_MoveToClick
Greatschools_Links_WebContents.CAPTCHA += Greatschools_Captcha
Greatschools_Links_WebContents.BADREQUEST += Greatschools_BadRequest
Greatschools_Links_WebContents.ZIPCODE += Greatschools_Zipcode
Greatschools_Links_WebContents.RESULTS += Greatschools_Results


class Greatschools_Links_WebPage(WebBrowserPage, contents=Greatschools_Links_WebContents): 
    def setup(self, *args, **kwargs):  
        if self.badrequest: raise BadRequestError(str(self))
        self.load[Greatschools_Links_WebContents.ZIPCODE](*args, **kwargs)
        self.load[Greatschools_Links_WebContents.RESULTS](*args, **kwargs)
        if not bool(self[Greatschools_Links_WebContents.ZIPCODE]): raise MulliganError(str(self))

    def execute(self, *args, **kwargs):
        query = {'dataset':'school', 'zipcode':str(self[Greatschools_Links_WebContents.ZIPCODE].data())}
        if not bool(self[Greatschools_Links_WebContents.RESULTS]): return    
        for content in iter(self):
            data = {'GID':content['link'].data(), 'address':content['address'].data(), 'link':content['link'].url}         
            yield query, 'links', data 
        nextpage = next(self)
        if nextpage: 
            nextpage.setup(*args, **kwargs)
            yield from nextpage(*args, **kwargs)
        else: return
 
             
class Greatschools_Links_WebCache(WebCache, querys=['dataset', 'zipcode'], dataset='links'): pass
class Greatschools_Links_WebQueue(WebQueue, querys=['dataset', 'zipcode'], dataset=['school']):      
    def zipcode(self, *args, state, county=None, countys=[], city=None, citys=[], **kwargs): 
        dataframe = self.load(QUEUE_FILE)
        assert all([isinstance(item, (str, type(None))) for item in (county, city)])
        assert all([isinstance(item, list) for item in (countys, citys)])
        countys = list(set([item for item in [county, *countys] if item]))
        citys = list(set([item for item in [city, *citys] if item]))          
        dataframe = dataframe_parser(dataframe, parsers={'zipcode':lambda x: '{:05.0f}'.format(int(x))}, defaultparser=str)
        dataframe = dataframe[['zipcode', 'type' ,'city', 'state', 'county']]
        dataframe = dataframe[dataframe['type'] == 'standard'][['zipcode', 'city', 'state', 'county']].reset_index(drop=True)
        if citys or countys: dataframe = dataframe[(dataframe['city'].isin(list(citys)) | dataframe['county'].isin(list(countys)))]
        if state: dataframe = dataframe[dataframe['state'] == state]             
        return list(dataframe['zipcode'].to_numpy())          
    
    
class Greatschools_Links_WebDownloader(WebDownloader, by='GID', delay=30, attempts=3):
    def execute(self, *args, queue, delayer, **kwargs): 
        with Greatschools_Links_WebDriver(DRIVER_FILE, browser='chrome', loadtime=50) as driver:
            webpage = Greatschools_Links_WebPage(driver, delayer=delayer)
            for feedquery in iter(queue):
                weburl = Greatschools_Links_WebURL(**feedquery)
                try: webpage.load(weburl, referer=None)
                except CaptchaError as error:
                    queue += feedquery
                    raise error  
                try: webpage.setup(*args, **kwargs)
                except MulliganError as error:
                    queue += feedquery
                    LOGGER.info(webpage.status)
                    raise error
                except BadRequestError: 
                    LOGGER.info("WebPage BadRequest: {}".format(str(webpage)))
                    LOGGER.info(str(weburl))
                    yield Greatschools_Links_WebCache(feedquery, {})
                    continue                
                for query, dataset, dataframe in webpage(*args, **kwargs): yield Greatschools_Links_WebCache(query, {dataset:dataframe}) 
                while webpage.crawl(queue):
                    webpage.setup(*args, **kwargs)
                    for query, dataset, dataframe in webpage(*args, **kwargs): yield Greatschools_Links_WebCache(query, {dataset:dataframe})        

    
def main(*args, **kwargs): 
    webdelayer = Greatschools_Links_WebDelayer('random', wait=(30, 60))
    webqueue = Greatschools_Links_WebQueue(REPORT_FILE, *args, days=30, **kwargs)
    webdownloader = Greatschools_Links_WebDownloader(REPO_DIR, REPORT_FILE, *args, delayer=webdelayer, queue=webqueue, attempts=1, **kwargs) 
    webdownloader(*args, **kwargs)
    LOGGER.info(str(webdownloader))
    for results in webdownloader.results: LOGGER.info(str(results))
    if not bool(webdownloader): raise webdownloader.error
    

if __name__ == '__main__': 
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={}, default=str)  
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)    
    

















