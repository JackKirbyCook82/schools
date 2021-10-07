# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Pages Download Application
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
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
SAVE_DIR = os.path.join(ROOT_DIR, "save")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
QUEUE_FILE = os.path.join(SAVE_DIR, "links.zip.csv")
REPORT_FILE = os.path.join(SAVE_DIR, "schools.csv")
USERAGENTS_FILE = os.path.join(RESOURCE_DIR, "useragents.zip.jl")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webtimers import WebDelayer
from webscraping.webreaders import WebReader, Retrys, UserAgents, Headers
from webscraping.weburl import WebURL
from webscraping.webpages import WebRequestPage
from webscraping.webpages import MissingRequestError
from webscraping.webpages import WebContents, WebCrawlingContents
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDatasets
from webscraping.webqueues import WebScheduler
from webscraping.webdownloaders import WebDownloader, CacheMixin, AttemptsMixin
from webscraping.webdata import WebText, WebLink
from webscraping.webvariables import Address, Price

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Schools_WebDelayer", "Greatschools_Schools_WebDownloader", "Greatschools_Schools_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


address_xpaths = []
district_xpaths = []
school_name_xpaths = []
school_type_xpaths = []
grades_xpaths = []
overall_score_xpaths = []
academic_score_xpaths = []
test_score_xpaths = []
subject_keys_xpaths = []
subject_values_xpaths = []
race_keys_xpaths = []
race_values_xpaths = []
graduation_xpaths = []
sat11_ready_xpaths = []
sat12_ready_xpaths = []
act_ready_xpaths = []
low_income_xpaths = []
no_english_xpaths = []
experience_xpaths = []
certification_xpaths = []
salary_xpaths = []
student_teachers_xpaths = []
crawler_xpaths = []


address_webloader = WebLoader(xpaths=address_xpaths)
district_webloader = WebLoader(xpaths=district_xpaths)
school_name_webloader = WebLoader(xpaths=school_name_xpaths)
school_type_webloader = WebLoader(xpaths=school_type_xpaths)
grades_webloader = WebLoader(xpaths=grades_xpaths)
overall_score_webloader = WebLoader(xpaths=overall_score_xpaths)
academic_score_webloader = WebLoader(xpaths=academic_score_xpaths)
test_score_webloader = WebLoader(xpaths=test_score_xpaths)
subject_keys_webloader = WebLoader(xpaths=subject_keys_xpaths)
subject_values_webloader = WebLoader(xpaths=subject_values_xpaths)
race_keys_webloader = WebLoader(xpaths=race_keys_xpaths)
race_values_webloader = WebLoader(xpaths=race_values_xpaths)
graduation_webloader = WebLoader(xpaths=graduation_xpaths)
sat11_ready_webloader = WebLoader(xpaths=sat11_ready_xpaths)
sat12_ready_webloader = WebLoader(xpaths=sat12_ready_xpaths)
act_ready_webloader = WebLoader(xpaths=act_ready_xpaths)
low_income_webloader = WebLoader(xpaths=low_income_xpaths)
no_english_webloader = WebLoader(xpaths=no_english_xpaths)
experience_webloader = WebLoader(xpaths=experience_xpaths)
certification_webloader = WebLoader(xpaths=certification_xpaths)
salary_webloader = WebLoader(xpaths=salary_xpaths)
student_teachers_webloader = WebLoader(xpaths=student_teachers_xpaths)
crawler_webloader = WebLoader(xpaths=crawler_xpaths)


identity_parser = lambda x: x.replace("https://www.greatschools.org/", "")
address_parser = lambda x: str(Address.fromsearch(x))
price_parser = lambda x: str(Price.fromsearch(x))
link_parser = lambda x: "".join(["https", "://", "www.greatschools.org", x]) if not str(x).startswith("".join(["https", "://", "www.greatschools.org"])) else x   
grades_parser = lambda x: str(x).strip().replace("-", "|")
score_parser = lambda x: int(str(x).strip().split("/")[0]) if bool(str(x).strip()) else None
percent_parser = lambda x: int(re.findall(r"\d+(?=\%$)", x)[0])
graph_percent_parser = lambda x: int(re.findall("(?<=Created with Highcharts \d\.\d\.\d)\d+(?=\%$)", x)[0])
ratio_parser = lambda x: float(Fraction(re.sub("[\s\n\r]+", "", x.strip()).replace(":", "/")))
crawler_keyparser = lambda x: hash(frozenset({"gid": identity_parser(x)}))
crawler_linkparser = lambda x: "".join(["https", "://", "www.greatschools.org", x]) if not str(x).startswith("".join(["https", "://", "www.greatschools.org"])) else x     


# NODES
class Greatschools_Address(WebText, loader=address_webloader, parsers={"data": address_parser}): pass
class Greatschools_District(WebText, loader=district_webloader, parsers={"link": link_parser}): pass
class Greatschools_SchoolName(WebText, loader=school_name_webloader): pass
class Greatschools_SchoolType(WebText, loader=school_type_webloader): pass
class Greatschools_Grades(WebText, loader=grades_webloader, parsers={"data": grades_parser}, optional=True): pass
class Greatschools_OverallScore(WebText, loader=overall_score_webloader, parsers={"data": score_parser}, optional=True): pass
class Greatschools_AcademicScore(WebText, loader=academic_score_webloader, parsers={"data": score_parser}, optional=True): pass
class Greatschools_TestScore(WebText, loader=test_score_webloader, parsers={"data": score_parser}, optional=True): pass
class Greatschools_SubjectKeys(WebText, loader=subject_keys_webloader, optional=True): pass
class Greatschools_SubjectValues(WebText, loader=subject_values_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_RaceKeys(WebText, loader=race_keys_webloader, optional=True): pass
class Greatschools_RaceValues(WebText, loader=race_values_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_Graduation(WebText, loader=graduation_webloader, parsesr={"data": percent_parser}, optional=True): pass
class Greatschools_SAT11(WebText, loader=sat11_ready_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_SAT12(WebText, loader=sat12_ready_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_ACT(WebText, loader=act_ready_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_LowIncome(WebText, loader=low_income_webloader, parsers={"data": graph_percent_parser}, optional=True): pass
class Greatschools_NoEnglish(WebText, loader=no_english_webloader, parsers={"data": graph_percent_parser}, optional=True): pass
class Greatschools_Experience(WebText, loader=experience_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_StudentTeachers(WebText, loader=student_teachers_webloader, parsers={"data": ratio_parser}, optional=True): pass
class Greatschools_Crawler(WebLink, loader=crawler_webloader, parsers={"key": crawler_keyparser, "link": crawler_linkparser}, optional=True): pass


class Greatschools_Schools_WebDelayer(WebDelayer): pass 
class Greatschools_Schools_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), headers=Headers(UserAgents.load(USERAGENTS_FILE, limit=100)), authenticate=None): pass
class Greatschools_Schools_WebURL(WebURL, protocol="https", domain="www.greatschools.org"): pass


class Greatschools_Schools_WebQuery(WebQuery, fields=["GID"]): pass
class Greatschools_Schools_WebDatasets(WebDatasets, fields=["location", "schools", "scores", "college", "testing", "teachers", "demographics"]): pass


class Greatschools_Schools_WebScheduler(WebScheduler, fields=["GID"]):
    def GID(self, *args, state, city=None, citys=[], zipcode=None, zipcodes=[], **kwargs):
        dataframe = self.load(QUEUE_FILE)
        assert all([isinstance(item, (str, type(None))) for item in (zipcode, city)])
        assert all([isinstance(item, list) for item in (zipcodes, citys)])
        zipcodes = list(set([item for item in [zipcode, *zipcodes] if item]))
        citys = list(set([item for item in [city, *citys] if item]))
        dataframe = dataframe_parser(dataframe, parsers={"address":Address.fromstr}, defaultparser=str)
        dataframe["city"] = dataframe["address"].apply(lambda x: x.city if x else None)
        dataframe["state"] = dataframe["address"].apply(lambda x: x.state if x else None)
        dataframe["zipcode"] = dataframe["address"].apply(lambda x: x.zipcode if x else None)
        if citys or zipcodes:
            dataframe = dataframe[(dataframe["city"].isin(list(citys)) | dataframe["zipcode"].isin(list(zipcodes)))]
        if state:
            dataframe = dataframe[dataframe["state"] == state]
        return list(dataframe["GID"].to_numpy())

    @staticmethod
    def execute(querys, *args, **kwargs): return [Greatschools_Schools_WebQuery(query) for query in querys]


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


class Greatschools_Schools_WebCrawlingContents(WebCrawlingContents): pass


Greatschools_Schools_WebContents.CRAWLER += Greatschools_Crawler


class Greatschools_Schools_WebPage(WebRequestPage):
    def setup(self, *args, **kwargs): 
        for content in iter(self.load):
            content(*args, **kwargs)
        if self.empty:
            self.show()

    @property
    def query(self): return {"GID": str(identity_parser(self.url))}

    def execute(self, *args, **kwargs):
        for data in self.location(*args, **kwargs):
            yield "location", data
        for data in self.schools(*args, **kwargs):
            yield "schools", data
        for data in self.scores(*args, **kwargs):
            yield "scores", data
        for data in self.college(*args, **kwargs):
            yield "college", data
        for data in self.teachers(*args, **kwargs):
            yield "teachers", data
        for data in self.testing(*args,args):
            yield "testing", data
        for data in self.demographics(*args, **kwargs):
            yield "demographics", data

    def location(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.ADDRESS, Greatschools_Schools_WebContents.DISTRICT)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        if bool(self[Greatschools_Schools_WebContents.ADDRESS]):
            data["address"] = self[Greatschools_Schools_WebContents.ADDRESS].data()
        if bool(self[Greatschools_Schools_WebContents.DISTRICT]):
            data["district"] = self[Greatschools_Schools_WebContents.DISTRICT].data()
            data["districtlink"] = self[Greatschools_Schools_WebContents.DISTRICT].link()
        yield data
    
    def schools(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SCHOOLNAME, Greatschools_Schools_WebContents.SCHOOLTYPE, Greatschools_Schools_WebContents.GRADES)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        if bool(self[Greatschools_Schools_WebContents.SCHOOLNAME]):
            data["schoolname"] = self[Greatschools_Schools_WebContents.SCHOOLNAME].data()
        if bool(self[Greatschools_Schools_WebContents.SCHOOLTYPE]):
            data["schooltype"] = self[Greatschools_Schools_WebContents.SCHOOLTYPE].data()
        if bool(self[Greatschools_Schools_WebContents.GRADES]):
            data["grades"] = self[Greatschools_Schools_WebContents.GRADES].data()
        yield data

    def scores(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.OVERALLSCORE, Greatschools_Schools_WebContents.ACADEMICSCORE, Greatschools_Schools_WebContents.TESTSCORE)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        if bool(self[Greatschools_Schools_WebContents.OVERALLSCORE]):
            data["overallscore"] = self[Greatschools_Schools_WebContents.OVERALLSCORE].data()
        if bool(self[Greatschools_Schools_WebContents.ACADEMICSCORE]):
            data["academicscore"] = self[Greatschools_Schools_WebContents.ACADEMICSCORE].data()
        if bool(self[Greatschools_Schools_WebContents.TESTSCORE]):
            data["testscore"] = self[Greatschools_Schools_WebContents.TESTSCORE].data()
        yield data
    
    def college(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.GRADUATION, Greatschools_Schools_WebContents.SAT11, Greatschools_Schools_WebContents.SAT12, Greatschools_Schools_WebContents.ACT)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        if bool(self[Greatschools_Schools_WebContents.GRADUATION]):
            data["graduation"] = self[Greatschools_Schools_WebContents.GRADUATION].data()
        if bool(self[Greatschools_Schools_WebContents.SAT11]):
            data["SAT11"] = self[Greatschools_Schools_WebContents.SAT11].data()
        if bool(self[Greatschools_Schools_WebContents.SAT12]):
            data["SAT12"] = self[Greatschools_Schools_WebContents.SAT12].data()
        if bool(self[Greatschools_Schools_WebContents.ACT]):
            data["ACT"] = self[Greatschools_Schools_WebContents.ACT].data()
        yield data        

    def teachers(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.EXPERIENCE, Greatschools_Schools_WebContents.STUDENTTEACHERS)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        if bool(self[Greatschools_Schools_WebContents.EXPERIENCE]):
            data["experience"] = self[Greatschools_Schools_WebContents.EXPERIENCE].data()
        if bool(self[Greatschools_Schools_WebContents.STUDENTTEACHERS]):
            data["studentteachers"] = self[Greatschools_Schools_WebContents.STUDENTTEACHERS].data()
        yield data
    
    def testing(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.SUBJECTKEYS, Greatschools_Schools_WebContents.SUBJECTVALUES)]):
            return
        data = {"GID": str(identity_parser(self.url))}
        assert len(self[Greatschools_Schools_WebContents.SUBJECTKEYS]) == len(self[Greatschools_Schools_WebContents.SUBJECTVALUES])
        data.update({key: str(value) for key, value in zip(self[Greatschools_Schools_WebContents.SUBJECTKEYS], self[Greatschools_Schools_WebContents.SUBJECTVALUES])})
        yield data

    def demographics(self):
        if not any([bool(self[content]) for content in (Greatschools_Schools_WebContents.RACEKEYS, Greatschools_Schools_WebContents.RACEVALUES, Greatschools_Schools_WebContents.LOWINCOME, Greatschools_Schools_WebContents.NOENGLISH)]):
            return
        assert len(self[Greatschools_Schools_WebContents.RACEKEYS]) == len(self[Greatschools_Schools_WebContents.RACEVALUES])
        data = {key: value for key, value in zip(self[Greatschools_Schools_WebContents.RACEKEYS], self[Greatschools_Schools_WebContents.RACEVALUES]) if key in ("White", "Hispanic", "Black", "Asian")}
        data["Other"] = max(100 - sum(list(data.values())), 0)
        data = {key: str(value) for key, value in data.items()}
        data.update({"GID": str(identity_parser(self.url))})
        yield data


class Greatschools_Schools_WebDownloader(WebDownloader + CacheMixin + AttemptsMixin, attempts=10, delay=25):
    def execute(self, *args, queue, delayer, **kwargs):
        with Greatschools_Schools_WebReader() as session:
            page = Greatschools_Schools_WebPage(session, delayer=delayer)
            for query in iter(queue):
                with query(lambda x: x.todict()) as items:
                    url = self.links[items["GID"]]
                    try:
                        page.load(url, referer=None)
                    except MissingRequestError:
                        yield query, Greatschools_Schools_WebDatasets({})
                        continue
                    page.setup(*args, **kwargs)
                    for dataset, data in page(*args, **kwargs):
                        yield query, Greatschools_Schools_WebDatasets({dataset: data})

#                while page.crawl(queue):
#                    page.setup(*args, **kwargs)
#                    for query, dataset, dataframe in page(*args, **kwargs):
#                        yield Greatschools_Schools_WebCache(query, {dataset:dataframe})

    @property
    def links(self, *args, **kwargs):
        dataframe = self.load(QUEUE_FILE)[["GID", "link"]]
        dataframe.set_index("GID", inplace=True)
        dataframe = dataframe.squeeze(axis=1)
        return dataframe.to_dict()


def main(*args, **kwargs): 
    delayer = Greatschools_Schools_WebDelayer("random", wait=(60, 120))
    scheduler = Greatschools_Schools_WebScheduler(*args, file=QUEUE_FILE, **kwargs)
    downloader = Greatschools_Schools_WebDownloader(*args, repository=REPOSITORY_DIR, **kwargs)
    queue = scheduler(*args, file=REPORT_FILE, **kwargs)
    downloader(*args, delayer=delayer, queue=queue, **kwargs)
    while True: 
        if downloader.off:
            break
        if downloader.error:
            break
        time.sleep(15)
    LOGGER.info(str(downloader))
    for results in downloader.results:
        print(str(results))
    if not bool(downloader):
        raise downloader.error


if __name__ == "__main__": 
    sys.argv += ["state=CA", "city=Bakersfield"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)
    



 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
