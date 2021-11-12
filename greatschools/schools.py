# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Pages Download Application
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
SAVE_DIR = os.path.join(ROOT_DIR, "save")
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
USERAGENTS_FILE = os.path.join(RESOURCE_DIR, "useragents.zip.jl")
QUEUE_FILE = os.path.join(SAVE_DIR, "links.zip.csv")
REPORT_FILE = os.path.join(SAVE_DIR, "schools.csv")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webtimers import WebDelayer, WebRuntime
from webscraping.webreaders import WebReader, Retrys, UserAgents, Headers
from webscraping.weburl import WebURL
from webscraping.webpages import WebRequestPage, WebDatas, webpage_bypass_error
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable
from webscraping.webdownloaders import WebDownloader, AttemptsMixin, CacheMixin
from webscraping.webdata import WebText, WebTexts, EmptyWebDataError
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Schools_WebDelayer", "Greatschools_Schools_WebRuntime", "Greatschools_Schools_WebDownloader", "Greatschools_Schools_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""
__project__ = {"website": "GreatSchools", "project": "Schools"}


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


address_xpath = "//div[contains(@class, 'address')]//div[@class='content']"
filtration_xpath = "//div[@class='community-breadcrumbs']//span[not(contains(@class, 'divider')) and not(./span)]"
name_xpath = "//h1[@class='school-name']"
type_xpath = "(//div[@class='school-stats-item'])[1]"
grades_xpath = "(//div[@class='school-stats-item'])[3]"
score_keys_xpath = "//div[@class='rating-snapshot']/a//div[contains(@class, 'title')]"
score_values_xpath = "//div[@class='rating-snapshot']/a//div[contains(@class, 'value')]"
test_keys_xpath = "//div[@id='Test_scores']//div[contains(@class, 'breakdown')]/span"
test_values_xpath = "//div[@id='Test_scores']//div[contains(@class, 'percentage')]"
test_averages_xpath = "//div[@id='Test_scores']//div[@class='state-average']"
demographic_keys_xpath = "//div[@id='Students']//div[contains(@class, 'breakdown')]/span"
demographic_values_xpath = "//div[@id='Students']//div[contains(@class, 'percentage')]"


address_webloader = WebLoader(xpath=address_xpath)
filtration_webloader = WebLoader(xpath=filtration_xpath)
name_webloader = WebLoader(xpath=name_xpath)
type_webloader = WebLoader(xpath=type_xpath)
grades_webloader = WebLoader(xpath=grades_xpath)
score_keys_webloader = WebLoader(xpath=score_keys_xpath)
score_keys_webloader = WebLoader(xpath=score_values_xpath)
test_keys_webloader = WebLoader(xpath=test_keys_xpath)
test_values_webloader = WebLoader(xpath=test_values_xpath)
test_averages_webloader = WebLoader(xpath=test_averages_xpath)
demographic_keys_webloader = WebLoader(xpath=demographic_keys_xpath)
demographic_values_webloader = WebLoader(xpath=demographic_values_xpath)


identity_pattern = "(?<=\/)\d{5}"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
address_parser = lambda x: str(Address.fromsearch(x))
grade_parser = lambda x: str(x).strip().replace("-", "|")
percent_parser = lambda x: int(re.findall(r"\d+(?=\%$)", x)[0])
score_parser = lambda x: int(str(x).strip().split("/")[0]) if bool(str(x).strip()) else None


# NODES
class Greatschools_Address(WebText, webloader=address_webloader, parsers={"data": address_parser}): pass
class Greatschools_Filtration(WebTexts, webloader=filtration_xpath): pass
class Greatschools_Name(WebText, webloader=name_webloader, parsers={"data": str}): pass
class Greatschools_Type(WebText, webloader=type_webloader, parsers={"data": str}): pass
class Greatschools_Grades(WebText, webloader=grades_webloader, parsers={"data": grade_parser}): pass
class Greatschools_ScoreKeys(WebTexts, webloader=score_keys_webloader, parsers={"data": str}): pass
class Greatschools_ScoreValues(WebTexts, webloader=score_values_xpath, parsers={"data": percent_parser}): pass
class Greatschools_TestKeys(WebTexts, webloader=test_keys_webloader, parsers={"data": str}): pass
class Greatschools_TestValues(WebTexts, webloader=test_values_webloader, parsers={"data": percent_parser}): pass
class Greatschools_TestAverages(WebTexts, webloader=test_averages_webloader, parsers={"data": percent_parser}): pass
class Greatschools_DemographicKeys(WebTexts, webloader=demographic_keys_webloader, parsers={"data": str}): pass
class Greatschools_DemographicValues(WebTexts, webloader=demographic_values_webloader, parsers={"data": percent_parser}): pass


class Greatschools_Schools_WebDelayer(WebDelayer): pass
class Greatschools_Schools_WebRuntime(WebRuntime): pass
class Greatschools_Schools_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), headers=Headers(UserAgents.load(USERAGENTS_FILE, limit=100)), authenticate=None): pass


class Greatschools_Schools_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, GID, name, address, **kwargs):
        return [address.state, address.city, "{GID}_{name}".format(GID=str(GID), name="-".join(str(name).split(" ")))]


class Greatschools_Schools_WebQuery(WebQuery, WebQueueable, fields=["GID"]): pass
class Greatschools_Schools_WebDataset(WebDataset, fields=["location", "scores", "testing", "demographics"]): pass


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


class Greatschools_Schools_WebData(WebDatas):
    FILTRATION = Greatschools_Filtration
    ADDRESS = Greatschools_Address
    NAME = Greatschools_Name
    TYPE = Greatschools_Type
    GRADES = Greatschools_Grades
    SCORE_KEYS = Greatschools_ScoreKeys
    SCORE_VALUES = Greatschools_ScoreValues
    TEST_KEYS = Greatschools_TestKeys
    TEST_VALUES = Greatschools_TestValues
    TEST_AVERAGES = Greatschools_TestAverages
    DEMOGRAPHICS_KEYS = Greatschools_DemographicKeys
    DEMOGRAPHICS_VALUES = Greatschools_DemographicValues


class Greatschools_Schools_WebPage(WebRequestPage, webdatas=Greatschools_Schools_WebData):
    def setup(self, *args, **kwargs): pass

    def execute(self, *args, **kwargs):
        query = self.query()
        yield query, "school", self.school()
        yield query, "scores", self.scores()
        yield query, "testing", self.testing()
        yield query, "demographics", self.demographics()

    def query(self): return {"GID": str(identity_parser(self.url))}

    @webpage_bypass_error(EmptyWebDataError, None)
    def school(self): return {**self.query(), "address": self[Greatschools_Schools_WebData.ADDRESS].data(), "name": self[Greatschools_Schools_WebData.NAME].data(), "type": self[Greatschools_Schools_WebData.TYPE].data(), "grade": self[Greatschools_Schools_WebData.GRADES].data()}
    @webpage_bypass_error(EmptyWebDataError, None)
    def scores(self): return {**self.query(), **{key: value for key, value in zip(self[Greatschools_Schools_WebData.SCORE_KEYS].data(), self[Greatschools_Schools_WebData.SCORE_VALUES].data())}}
    @webpage_bypass_error(EmptyWebDataError, None)
    def testing(self): return {**self.query(), **{key: value for key, value in zip(self[Greatschools_Schools_WebData.TEST_KEYS].data(), self[Greatschools_Schools_WebData.TEST_VALUES].data())}}
    @webpage_bypass_error(EmptyWebDataError, None)
    def demographics(self): return {**self.query(), **{key: value for key, value in zip(self[Greatschools_Schools_WebData.DEMOGRAPHICS_KEYS].data(), self[Greatschools_Schools_WebData.DEMOGRAPHICS_VALUES].data())}}


class Greatschools_Schools_WebDownloader(AttemptsMixin, CacheMixin, WebDownloader, attempts=10, sleep=5, **__project__):
    def execute(self, *args, queue, delayer, **kwargs):
        with Greatschools_Schools_WebReader() as session:
            page = Greatschools_Schools_WebPage(session, delayer=delayer)
            with queue:
                for query in iter(queue):
                    with query:
                        url = Greatschools_Schools_WebURL.fromstr(str(self.get(**query.todict())))
                        page.load(url, referer=None)
                        page.setup(*args, **kwargs)
                        for fields, dataset, data in page(*args, **kwargs):
                            yield Greatschools_Schools_WebQuery(fields), Greatschools_Schools_WebDataset({dataset: data})
                        yield from page(*args, **kwargs)

    def get(self, *args, GID, **kwargs):
        dataframe = self.load(QUEUE_FILE)[["GID", "link"]]
        dataframe.set_index("GID", inplace=True)
        series = dataframe.squeeze(axis=1)
        return series.get(GID)


def main(*args, **kwargs): 
    delayer = Greatschools_Schools_WebDelayer("random", wait=(30, 120))
    scheduler = Greatschools_Schools_WebScheduler(*args, file=REPORT_FILE, **kwargs)
    downloader = Greatschools_Schools_WebDownloader(*args, repository=REPOSITORY_DIR, **kwargs)
    queue = scheduler(*args, **kwargs)
    downloader(*args, delayer=delayer, queue=queue, **kwargs)
    LOGGER.info(repr(downloader))
    for query, results in downloader.results:
        LOGGER.info(str(query))
        LOGGER.info(str(results))
    if not bool(downloader):
        raise downloader.error


if __name__ == "__main__":
    sys.argv += ["state=CA", "city=Bakersfield", "proxycap=10"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)




 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
