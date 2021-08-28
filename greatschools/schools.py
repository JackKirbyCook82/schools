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

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MOD_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MOD_DIR, os.pardir))
RES_DIR = os.path.join(ROOT_DIR, 'resources')
SAVE_DIR = os.path.join(ROOT_DIR, 'save')
USERAGENTS_FILE = os.path.join(RES_DIR, 'useragents.zip.jl')
REPO_DIR = os.path.join(SAVE_DIR, 'greatschools')
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


address_xpaths = []
district_xpaths = []
schoolname_xpaths = []
schooltype_xpaths = []
grades_xpaths = []
overallscore_xpaths = []
academicscore_xpaths = []
testscore_xpaths = []
subjectkeys_xpaths = []
subjectvalues_xpaths = []
racekeys_xpaths = []
racevalues_xpaths = []
graduation_xpaths = []
sat11ready_xpaths = []
sat12ready_xpaths = []
actready_xpaths = []
lowincome_xpaths = []
noenglish_xpaths = []
experience_xpaths = []
certification_xpaths = []
salary_xpaths = []
studentteachers_xpaths = [] 
crawler_xpaths = []


def GID_parser(string): return string.replace("https://www.greatschools.org/", "")

address_parser = lambda x: str(Address.fromsearch(x))
price_parser = lambda x: str(Price.fromsearch(x))
link_parser = lambda x: ''.join(['https', '://', 'www.greatschools.org', x]) if not str(x).startswith(''.join(['https', '://', 'www.greatschools.org'])) else x   
grades_parser = lambda x: str(x).strip().replace('-', '|')
score_parser = lambda x: int(str(x).strip().split('/')[0]) if bool(str(x).strip()) else None
percent_parser = lambda x: int(re.findall(r"\d+(?=\%$)", x)[0])
graphpercent_parser = lambda x: int(re.findall("(?<=Created with Highcharts \d\.\d\.\d)\d+(?=\%$)", x)[0])
ratio_parser = lambda x: float(Fraction(re.sub('[\s\n\r]+', '', x.strip()).replace(':', '/')))
crawler_keyparser = lambda x: hash(frozenset({'GID':GID_parser(x)}))
crawler_linkparser = lambda x: ''.join(['https', '://', 'www.greatschools.org', x]) if not str(x).startswith(''.join(['https', '://', 'www.greatschools.org'])) else x     


class Greatschools_Address(WebText.update(parsers={'data':address_parser}), xpaths=address_xpaths): pass
class Greatschools_District(WebText.update(parsers={'link':link_parser}), xpaths=district_xpaths): pass
class Greatschools_SchoolName(WebText, xpaths=schoolname_xpaths): pass
class Greatschools_SchoolType(WebText, xpaths=schooltype_xpaths): pass
class Greatschools_Grades(WebText.update(parsers={'data':grades_parser}), xpaths=grades_xpaths): pass
class Greatschools_OverallScore(WebText.update(parsers={'data':score_parser}), xpaths=overallscore_xpaths): pass
class Greatschools_AcademicScore(WebText.update(parsers={'data':score_parser}), xpaths=academicscore_xpaths): pass
class Greatschools_TestScore(WebText.update(parsers={'data':score_parser}), xpaths=testscore_xpaths): pass
class Greatschools_SubjectKeys(WebTextList, xpaths=subjectkeys_xpaths): pass
class Greatschools_SubjectValues(WebTextList.update(parsers={'data':percent_parser}), xpaths=subjectvalues_xpaths): pass
class Greatschools_RaceKeys(WebTextList, xpaths=racekeys_xpaths): pass
class Greatschools_RaceValues(WebTextList.update(parsers={'data':percent_parser}), xpaths=racevalues_xpaths): pass
class Greatschools_Graduation(WebText.update(parsesr={'data':percent_parser}), xpaths=graduation_xpaths): pass
class Greatschools_SAT11(WebText.update(parsers={'data':percent_parser}), xpaths=sat11ready_xpaths): pass
class Greatschools_SAT12(WebText.update(parsers={'data':percent_parser}), xpaths=sat12ready_xpaths): pass
class Greatschools_ACT(WebText.update(parsers={'data':percent_parser}), xpaths=actready_xpaths): pass
class Greatschools_LowIncome(WebText.update(parsers={'data':graphpercent_parser}), xpaths=lowincome_xpaths): pass
class Greatschools_NoEnglish(WebText.update(parsers={'data':graphpercent_parser}), xpaths=noenglish_xpaths): pass
class Greatschools_Experience(WebText.update(parsers={'data':percent_parser}), xpaths=experience_xpaths): pass
class Greatschools_StudentTeachers(WebText.update(parsers={'data':ratio_parser}), xpaths=studentteachers_xpaths): pass
class Greatschools_Crawler(WebLinkDict.update(parsers={'key':crawler_keyparser, 'link':crawler_linkparser}), xpaths=crawler_xpaths): pass

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
        if bool(self[Greatschools_Schools_WebContents.ADDRESS]): data['address'] = self[Greatschools_Schools_WebContents.ADDRESS].data()
        if bool(self[Greatschools_Schools_WebContents.DISTRICT]):
            data['district'] = self[Greatschools_Schools_WebContents.DISTRICT].data()
            data['districtlink'] = self[Greatschools_Schools_WebContents.DISTRICT].link()
        yield data
    
    def schools(self, *args, GID, **kwargs): 
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SCHOOLNAME, Greatschools_Schools_WebContents.SCHOOLTYPE, Greatschools_Schools_WebContents.GRADES)]): return          
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.SCHOOLNAME]): data['schoolname'] = self[Greatschools_Schools_WebContents.SCHOOLNAME].data()
        if bool(self[Greatschools_Schools_WebContents.SCHOOLTYPE]): data['schooltype'] = self[Greatschools_Schools_WebContents.SCHOOLTYPE].data()
        if bool(self[Greatschools_Schools_WebContents.GRADES]): data['grades'] = self[Greatschools_Schools_WebContents.GRADES].data()       
        yield data

    def scores(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.OVERALLSCORE, Greatschools_Schools_WebContents.ACADEMICSCORE, Greatschools_Schools_WebContents.TESTSCORE)]): return         
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.OVERALLSCORE]): data['overallscore'] = self[Greatschools_Schools_WebContents.OVERALLSCORE].data()
        if bool(self[Greatschools_Schools_WebContents.ACADEMICSCORE]): data['academicscore'] = self[Greatschools_Schools_WebContents.ACADEMICSCORE].data()
        if bool(self[Greatschools_Schools_WebContents.TESTSCORE]): data['testscore'] = self[Greatschools_Schools_WebContents.TESTSCORE].data()
        yield data
    
    def college(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.GRADUATION, Greatschools_Schools_WebContents.SAT11, Greatschools_Schools_WebContents.SAT12, Greatschools_Schools_WebContents.ACT)]): return  
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.GRADUATION]): data['graduation'] = self[Greatschools_Schools_WebContents.GRADUATION].data()
        if bool(self[Greatschools_Schools_WebContents.SAT11]): data['SAT11'] = self[Greatschools_Schools_WebContents.SAT11].data()
        if bool(self[Greatschools_Schools_WebContents.SAT12]): data['SAT12'] = self[Greatschools_Schools_WebContents.SAT12].data()
        if bool(self[Greatschools_Schools_WebContents.ACT]): data['ACT'] = self[Greatschools_Schools_WebContents.ACT].data()
        yield data        

    def teachers(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.EXPERIENCE, Greatschools_Schools_WebContents.STUDENTTEACHERS)]): return        
        data = {'GID':GID}
        if bool(self[Greatschools_Schools_WebContents.EXPERIENCE]): data['experience'] = self[Greatschools_Schools_WebContents.EXPERIENCE].data()         
        if bool(self[Greatschools_Schools_WebContents.STUDENTTEACHERS]): data['studentteachers'] = self[Greatschools_Schools_WebContents.STUDENTTEACHERS].data()
        yield data
    
    def testing(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SUBJECTKEYS, Greatschools_Schools_WebContents.SUBJECTVALUES)]): return           
        data = {'GID':GID}        
        assert len(self[Greatschools_Schools_WebContents.SUBJECTKEYS]) == len(self[Greatschools_Schools_WebContents.SUBJECTVALUES])
        data.update({key:str(value) for key, value in zip(self[Greatschools_Schools_WebContents.SUBJECTKEYS], self[Greatschools_Schools_WebContents.SUBJECTVALUES])})
        yield data

    def demographics(self, *args, GID, **kwargs):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.RACEKEYS, Greatschools_Schools_WebContents.RACEVALUES, Greatschools_Schools_WebContents.LOWINCOME, Greatschools_Schools_WebContents.NOENGLISH)]): return      
        assert len(self[Greatschools_Schools_WebContents.RACEKEYS]) == len(self[Greatschools_Schools_WebContents.RACEVALUES])
        data = {key:value for key, value in zip(self[Greatschools_Schools_WebContents.RACEKEYS], self[Greatschools_Schools_WebContents.RACEVALUES]) if key in ('White', 'Hispanic', 'Black', 'Asian')}
        data['Other'] = max(100 - sum(list(data.values())), 0)
        data = {key:str(value) for key, value in data.items()}
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
    webdownloader = Greatschools_Schools_WebDownloader(REPO_DIR, REPORT_FILE, *args, delayer=webdelayer, queue=webqueue, **kwargs)
    webdownloader(*args, **kwargs)
    while True: 
        if webdownloader.off: break
        if webdownloader.error: break
        time.sleep(15)
    LOGGER.info(str(webdownloader))
    for results in webdownloader.results: print(str(results))
    if not bool(webdownloader): raise webdownloader.error


if __name__ == '__main__': 
    sys.argv += ['city=Bakersfield', 'state=CA']
    logging.basicConfig(level='INFO', format="[%(levelname)s, %(threadName)s]:  %(message)s")
    inputparser = InputParser(proxys={'assign':'=', 'space':'_'}, parsers={}, default=str)  
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)    
    



 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
