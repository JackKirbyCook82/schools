# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Download Pages Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import time
import warnings
import logging
import regex as re
from fractions import Fraction

DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
RES_DIR = os.path.join(ROOT_DIR, 'resources')
SAVE_DIR = os.path.join(ROOT_DIR, 'save')
USERAGENTS_FILE = os.path.join(RES_DIR, 'useragents.zip.jl')
REPOSITORY_DIR = os.path.join(SAVE_DIR, 'greatschools')
QUEUE_FILE = os.path.join(SAVE_DIR, 'greatschools', 'links.zip.csv')
REPORT_FILE = os.path.join(SAVE_DIR, 'greatschools', 'schools.csv')
if not ROOT_DIR in sys.path: sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webapi import WebURL, WebCache, WebQueue, WebDownloader
from webscraping.webreaders import WebReader, Retrys, UserAgents, Headers
from webscraping.webtimers import WebDelayer
from webscraping.webpages import WebRequestPage, MissingRequestError, WebContents
from webscraping.webdata import WebText, WebTextList, WebLinkDict
from webscraping.webvariables import Address, Price

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['Greatschools_Schools_WebDelayer', 'Greatschools_Schools_WebDownloader', 'Greatschools_Schools_WebQueue']
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


address_xpath = r"(//div[contains(@class, 'address')]//span[@class='content'])[1]"
district_xpath = r"//div[contains(@class, 'district')]/a"
schoolname_xpath = r"//h1[@class='school-name']"
schooltype_xpath = r"(//div[@class='school-info']/div[contains(@class, 'item') and ./div[@class='label']/text()='Type']/div)[2]"
grades_xpath = r"(//div[@class='school-info']/div[contains(@class, 'item') and ./div[@class='label']/text()='Grades']/div)[2]"
overallscore_xpath = r"//div[contains(@class, 'rating-with-label__rating')]"
academicscore_xpath = r"//div[contains(@class, 'toc-entry') and ./a[@data-ga-click-label='Academic progress']]/a/span[contains(@class, 'gs-rating circle')]"
testscore_xpath = r"//div[contains(@class, 'toc-entry') and ./a[@data-ga-click-label='Test scores']]/a/span[contains(@class, 'gs-rating circle')]"
subjectkeys_xpath = r"//div[@id='TestScores']//div[contains(@class, 'test-score-container')]//div[contains(@class, 'subject') and not(./span)]"
subjectvalues_xpath = r"//div[@id='TestScores']//div[contains(@class, 'test-score-container')]//div[contains(@class, 'score') and not(./span)]"
racekeys_xpath = r"//div[@id='Students']//div[contains(@class, 'legend-separator')]/div[@class='legend-title'][1]"
racevalues_xpath = r"//div[@id='Students']//div[contains(@class, 'legend-separator')]/div[@class='legend-title'][2]"
graduation_xpath = r"//div[@id='College_readiness']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'graduation rate')]//div[@class='score']"
sat11ready_xpath = r"//div[@id='College_readiness']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'SAT 11th')]//div[@class='score']"
sat12ready_xpath = r"//div[@id='College_readiness']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'SAT 12th')]//div[@class='score']"
actready_xpath = r"//div[@id='College_readiness']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'ACT college readiness rate')]//div[@class='score']"
lowincome_xpath = r"//div[@id='Students']//div[@class='subgroups']//div[contains(@class, 'subgroup')]//div[contains(@id, 'lunch-program')]//div[contains(@id, 'highcharts')]"
noenglish_xpath = r"//div[@id='Students']//div[@class='subgroups']//div[contains(@class, 'subgroup')]//div[@id='english-learners']//div[contains(@id, 'highcharts')]"
experience_xpath = r"//div[@id='TeachersStaff']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'experience')]//div[@class='score']"
certification_xpath = r"//div[@id='TeachersStaff']//div[contains(@class, 'test-score-container') and contains(./div[contains(@class, 'subject')], 'certified')]//div[@class='score']"
salary_xpath = r"//div[@id='TeachersStaff']//div[@class='rating-score-item']/div[@class='row' and contains(./div[contains(@class, 'label')], 'Average teacher salary')]/div/div[contains(@class, 'rating-score-item')][1]"
studentteachers_xpath = r"//div[@id='TeachersStaff']//div[@class='rating-score-item']/div[@class='row' and contains(./div[contains(@class, 'label')], 'Students per teacher')]/div/div[contains(@class, 'rating-score-item')][1]"
crawler_xpath = r"//div[@id='NearbySchools']//div[@class='nearby-schools']//a[contains(@class, 'Click')]"


def GID_parser(string): return string.replace("https://www.greatschools.org/", "")

address_parser = lambda x: Address.fromsearch(x)
price_parser = lambda x: Price.fromsearch(x)
link_parser = lambda x: ''.join(['https', '://', 'www.greatschools.org', x]) if not str(x).startswith(''.join(['https', '://', 'www.greatschools.org'])) else x   
grades_parser = lambda x: str(x).strip().replace('-', '|')
score_parser = lambda x: int(str(x).strip().split('/')[0]) if bool(str(x).strip()) else None
percent_parser = lambda x: int(re.findall(r"\d+(?=\%$)", x)[0])
graphpercent_parser = lambda x: int(re.findall("(?<=Created with Highcharts \d\.\d\.\d)\d+(?=\%$)", x)[0])
ratio_parser = lambda x: float(Fraction(re.sub('[\s\n\r]+', '', x.strip()).replace(':', '/')))
crawler_keyparser = lambda x: hash(frozenset({'GID':GID_parser(x)}))
crawler_linkparser = lambda x: ''.join(['https', '://', 'www.greatschools.org', x]) if not str(x).startswith(''.join(['https', '://', 'www.greatschools.org'])) else x     


class Greatschools_Address(WebText.update(dataparser=address_parser), xpath=address_xpath): pass
class Greatschools_District(WebText.update(linkparser=link_parser), xpath=district_xpath): pass
class Greatschools_SchoolName(WebText, xpath=schoolname_xpath): pass
class Greatschools_SchoolType(WebText, xpath=schooltype_xpath): pass
class Greatschools_Grades(WebText.update(dataparser=grades_parser), xpath=grades_xpath): pass
class Greatschools_OverallScore(WebText.update(dataparser=score_parser), xpath=overallscore_xpath): pass
class Greatschools_AcademicScore(WebText.update(dataparser=score_parser), xpath=academicscore_xpath): pass
class Greatschools_TestScore(WebText.update(dataparser=score_parser), xpath=testscore_xpath): pass
class Greatschools_SubjectKeys(WebTextList, xpath=subjectkeys_xpath): pass
class Greatschools_SubjectValues(WebTextList.update(dataparser=percent_parser), xpath=subjectvalues_xpath): pass
class Greatschools_RaceKeys(WebTextList, xpath=racekeys_xpath): pass
class Greatschools_RaceValues(WebTextList.update(dataparser=percent_parser), xpath=racevalues_xpath): pass
class Greatschools_Graduation(WebText.update(dataparser=percent_parser), xpath=graduation_xpath): pass
class Greatschools_SAT11(WebText.update(dataparser=percent_parser), xpath=sat11ready_xpath): pass
class Greatschools_SAT12(WebText.update(dataparser=percent_parser), xpath=sat12ready_xpath): pass
class Greatschools_ACT(WebText.update(dataparser=percent_parser), xpath=actready_xpath): pass
class Greatschools_LowIncome(WebText.update(dataparser=graphpercent_parser), xpath=lowincome_xpath): pass
class Greatschools_NoEnglish(WebText.update(dataparser=graphpercent_parser), xpath=noenglish_xpath): pass
class Greatschools_Experience(WebText.update(dataparser=percent_parser), xpath=experience_xpath): pass
class Greatschools_StudentTeachers(WebText.update(dataparser=ratio_parser), xpath=studentteachers_xpath): pass
class Greatschools_Crawler(WebLinkDict.update(keyparser=crawler_keyparser, linkparser=crawler_linkparser), xpath=crawler_xpath): pass

class Greatschools_Schools_WebDelayer(WebDelayer): pass 
class Greatschools_Schools_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), headers=Headers(UserAgents.load(USERAGENTS_FILE, limit=100)), authenticate=None): pass
class Greatschools_Schools_WebURL(WebURL, protocol='https', domain='www.greatschools.org'): pass
        
       
class Greatschools_Schools_WebContents(WebContents): 
    ADDRESS = Greatschools_Address
    DISTRICT = Greatschools_District
    SCHOOLNAME = Greatschools_SchoolName
    SCHOOLTYPE = Greatschools_SchoolType
    GRADES = Greatschools_Grades
    OVERALLSCORE = Greatschools_OverallScore 
    ACADEMICSCORE = Greatschools_AcademicScore
    TESTSCORE = Greatschools_TestScore
    SUBJECTKEYS = Greatschools_SubjectKeys
    SUBJECTVALUES = Greatschools_SubjectValues
    RACEKEYS = Greatschools_RaceKeys
    RACEVALUES = Greatschools_RaceValues
    LOWINCOME = Greatschools_LowIncome
    NOENGLISH = Greatschools_NoEnglish
    GRADUATION = Greatschools_Graduation
    SAT11 = Greatschools_SAT11
    SAT12 = Greatschools_SAT12
    ACT = Greatschools_ACT
    EXPERIENCE = Greatschools_Experience 
    STUDENTTEACHERS = Greatschools_StudentTeachers
     
Greatschools_Schools_WebContents.CRAWLER += Greatschools_Crawler


class Greatschools_Schools_WebPage(WebRequestPage, contents=Greatschools_Schools_WebContents):    
    def setup(self, *args, **kwargs): 
        for content in iter(self.load): content(*args, **kwargs)
        if self.empty: self.show()
    
    def execute(self, *args, **kwargs): 
        GID = str(GID_parser(self.url))  
        query = {'GID':GID}
        for data in self.location(*args, GID=GID, **kwargs): yield query, 'location', data
        for data in self.schools(*args, GID=GID, **kwargs): yield query, 'schools', data
        for data in self.scores(*args, GID=GID, **kwargs): yield query, 'scores', data
        for data in self.college(*args, GID=GID, **kwargs): yield query, 'college', data
        for data in self.teachers(*args, GID=GID, **kwargs): yield query, 'teachers', data
        for data in self.testing(*args, GID=GID, **kwargs): yield query, 'testing', data
        for data in self.demographics(*args, GID=GID, **kwargs): yield query, 'demographics', data        

    def location(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.ADDRESS, Greatschools_Schools_WebContents.DISTRICT)]): return  
        data = {'GID':GID}    
        if bool(self[Greatschools_Schools_WebContents.ADDRESS]): data['address'] = str(self[Greatschools_Schools_WebContents.ADDRESS].data())
        if bool(self[Greatschools_Schools_WebContents.DISTRICT]):
            data['district'] = str(self[Greatschools_Schools_WebContents.DISTRICT].data())
            data['districtlink'] = str(self[Greatschools_Schools_WebContents.DISTRICT].link())
        yield data
    
    def schools(self, *args, GID, **kwargs): 
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SCHOOLNAME, Greatschools_Schools_WebContents.SCHOOLTYPE, Greatschools_Schools_WebContents.GRADES)]): return          
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.SCHOOLNAME]): data['schoolname'] = str(self[Greatschools_Schools_WebContents.SCHOOLNAME].data())
        if bool(self[Greatschools_Schools_WebContents.SCHOOLTYPE]): data['schooltype'] = str(self[Greatschools_Schools_WebContents.SCHOOLTYPE].data())
        if bool(self[Greatschools_Schools_WebContents.GRADES]): data['grades'] = str(self[Greatschools_Schools_WebContents.GRADES].data())       
        yield data

    def scores(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.OVERALLSCORE, Greatschools_Schools_WebContents.ACADEMICSCORE, Greatschools_Schools_WebContents.TESTSCORE)]): return         
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.OVERALLSCORE]): data['overallscore'] = str(self[Greatschools_Schools_WebContents.OVERALLSCORE].data())
        if bool(self[Greatschools_Schools_WebContents.ACADEMICSCORE]): data['academicscore'] = str(self[Greatschools_Schools_WebContents.ACADEMICSCORE].data())
        if bool(self[Greatschools_Schools_WebContents.TESTSCORE]): data['testscore'] = str(self[Greatschools_Schools_WebContents.TESTSCORE].data())
        yield data
    
    def college(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.GRADUATION, Greatschools_Schools_WebContents.SAT11, Greatschools_Schools_WebContents.SAT12, Greatschools_Schools_WebContents.ACT)]): return  
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.GRADUATION]): data['graduation'] = str(self[Greatschools_Schools_WebContents.GRADUATION].data())
        if bool(self[Greatschools_Schools_WebContents.SAT11]): data['SAT11'] = str(self[Greatschools_Schools_WebContents.SAT11].data())
        if bool(self[Greatschools_Schools_WebContents.SAT12]): data['SAT12'] = str(self[Greatschools_Schools_WebContents.SAT12].data())
        if bool(self[Greatschools_Schools_WebContents.ACT]): data['ACT'] = str(self[Greatschools_Schools_WebContents.ACT].data())
        yield data        

    def teachers(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.EXPERIENCE, Greatschools_Schools_WebContents.STUDENTTEACHERS)]): return        
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.EXPERIENCE]): data['experience'] = str(self[Greatschools_Schools_WebContents.EXPERIENCE].data())           
        if bool(self[Greatschools_Schools_WebContents.STUDENTTEACHERS]): data['studentteachers'] = str(self[Greatschools_Schools_WebContents.STUDENTTEACHERS].data())
        yield data
    
    def testing(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SUBJECTKEYS, Greatschools_Schools_WebContents.SUBJECTVALUES)]): return           
        data = {'GID':GID}        
        assert len(self[Greatschools_Schools_WebContents.SUBJECTKEYS]) == len(self[Greatschools_Schools_WebContents.SUBJECTVALUES])
        data.update({str(key):str(value) for key, value in zip(self[Greatschools_Schools_WebContents.SUBJECTKEYS], self[Greatschools_Schools_WebContents.SUBJECTVALUES])})
        yield data

    def demographics(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.RACEKEYS, Greatschools_Schools_WebContents.RACEVALUES, Greatschools_Schools_WebContents.LOWINCOME, Greatschools_Schools_WebContents.NOENGLISH)]): return      
        assert len(self[Greatschools_Schools_WebContents.RACEKEYS]) == len(self[Greatschools_Schools_WebContents.RACEVALUES])
        data = {key:value for key, value in zip(self[Greatschools_Schools_WebContents.RACEKEYS], self[Greatschools_Schools_WebContents.RACEVALUES]) if key in ('White', 'Hispanic', 'Black', 'Asian')}
        data['Other'] = max(100 - sum(list(data.values())), 0)
        data = {str(key):str(value) for key, value in data.items()}
        data.update({'GID':GID})
        yield data 
        

class Greatschools_Schools_WebCache(WebCache, query='GID', datasets=['location', 'schools', 'scores', 'college', 'testing', 'teachers', 'demographics']): pass
class Greatschools_Schools_WebQueue(WebQueue, query='GID'):
    def GID(self, *args, state, city=None, citys=[], zipcode=None, zipcodes=[], **kwargs):
        dataframe = self.load(QUEUE_FILE)
        assert all([isinstance(item, (str, type(None))) for item in (zipcode, city)])
        assert all([isinstance(item, list) for item in (zipcodes, citys)])
        zipcodes = list(set([item for item in [zipcode, *zipcodes] if item]))
        citys = list(set([item for item in [city, *citys] if item]))                 
        dataframe = dataframe_parser(dataframe, parsers={'address':Address.fromstr}, defaultparser=str)
        dataframe['city'] = dataframe['address'].apply(lambda x: x.city if x else None)
        dataframe['state'] = dataframe['address'].apply(lambda x: x.state if x else None)
        dataframe['zipcode'] = dataframe['address'].apply(lambda x: x.zipcode if x else None)         
        if citys or zipcodes: dataframe = dataframe[(dataframe['city'].isin(list(citys)) | dataframe['zipcode'].isin(list(zipcodes)))]
        if state: dataframe = dataframe[dataframe['state'] == state]     
        return list(dataframe['GID'].to_numpy())      


class Greatschools_Schools_WebDownloader(WebDownloader, delay=30, attempts=10):
    def execute(self, *args, queue, delayer, **kwargs):     
        links = self.links(*args, **kwargs)
        with Greatschools_Schools_WebReader() as session:
            webpage = Greatschools_Schools_WebPage(session, delayer=delayer)
            for feedquery in iter(queue):
                weburl = links[feedquery['GID']]
                try: webpage.load(weburl, referer="www.google.com")                    
                except MissingRequestError: 
                    LOGGER.info("WebPage Missing: {}".format(str(webpage)))
                    LOGGER.info(str(weburl))
                    yield Greatschools_Schools_WebCache(feedquery, {})
                    continue  
                webpage.setup(*args, **kwargs)               
                for query, dataset, dataframe in webpage(*args, **kwargs): yield Greatschools_Schools_WebCache(query, {dataset:dataframe})                     
                while webpage.crawl(queue):
                    webpage.setup(*args, **kwargs)
                    for query, dataset, dataframe in webpage(*args, **kwargs): yield Greatschools_Schools_WebCache(query, {dataset:dataframe})                

    def links(self, *args, **kwargs):
        dataframe = self.load(QUEUE_FILE)[['GID', 'link']]
        dataframe.set_index('GID', inplace=True)
        dataframe = dataframe.squeeze(axis=1)
        return dataframe.to_dict()


def main(*args, **kwargs): 
    webdelayer = Greatschools_Schools_WebDelayer('random', wait=(60, 120))
    webqueue = Greatschools_Schools_WebQueue(REPORT_FILE, *args, days=30, **kwargs)
    webdownloader = Greatschools_Schools_WebDownloader(REPOSITORY_DIR, REPORT_FILE, *args, delayer=webdelayer, queue=webqueue, **kwargs)
    webdownloader(*args, **kwargs)
    while True: 
        if webdownloader.off: break
        if webdownloader.error: break
        time.sleep(15)
    LOGGER.info(str(webdownloader))
    for results in webdownloader.results: print(str(results))
    if not bool(webdownloader): raise webdownloader.error


if __name__ == '__main__': 
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={}, default=str)  
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)    
    



 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
