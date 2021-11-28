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
from abc import ABC
from threading import Thread

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MOD_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MOD_DIR, os.pardir))
SAVE_DIR = os.path.join(ROOT_DIR, "save")
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
USERAGENTS_FILE = os.path.join(RESOURCE_DIR, "useragents.zip.jl")
DRIVER_EXE = os.path.join(RESOURCE_DIR, "chromedriver.exe")
NORDVPN_EXE = os.path.join("C:/", "Program Files", "NordVPN", "NordVPN.exe")
QUEUE_FILE = os.path.join(REPOSITORY_DIR, "links.zip.csv")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "schools.csv")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webtimers import WebDelayer
from webscraping.webvpn import NordWebVPN
from webscraping.webdrivers import WebBrowser
from webscraping.webreaders import WebReader, Retrys, UserAgents, Headers
from webscraping.weburl import WebURL
from webscraping.webpages import WebContentPage, WebPageContents, RefusalError, CaptchaError, BadRequestError, webpage_bypass
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable
from webscraping.webdownloaders import WebDownloader, CacheMixin, VPNMixin
from webscraping.webdata import WebText, WebTexts
from webscraping.webactions import WebScroll
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Schools_WebDelayer", "Greatschools_Schools_WebDownloader", "Greatschools_Schools_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""
__project__ = {"website": "GreatSchools", "project": "Schools"}


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


address_xpath = "//div[contains(@class, 'address')]//div[@class='content']"
filtration_xpath = "//div[@class='community-breadcrumbs']//span[not(contains(@class, 'divider')) and not(./span)]"
name_xpath = "//h1[@class='school-name']"
details_xpath = "//div[@class='hero-stats-layout']"
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
details_webloader = WebLoader(xpath=details_xpath)
score_keys_webloader = WebLoader(xpath=score_keys_xpath)
score_values_webloader = WebLoader(xpath=score_values_xpath)
test_keys_webloader = WebLoader(xpath=test_keys_xpath)
test_values_webloader = WebLoader(xpath=test_values_xpath)
test_averages_webloader = WebLoader(xpath=test_averages_xpath)
demographic_keys_webloader = WebLoader(xpath=demographic_keys_xpath)
demographic_values_webloader = WebLoader(xpath=demographic_values_xpath)


identity_pattern = "(?<=\/)\d+"
type_pattern = "[A-Za-z]+(?= school)"
grade_pattern = "(?<=Grades |Grade )[0-9A-Z-]+"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
address_parser = lambda x: str(Address.fromsearch(x))
key_parser = lambda x: str(x).strip()
type_parser = lambda x: str(re.findall(type_pattern, x)[0]).lower().title()
grade_parser = lambda x: str(re.findall(grade_pattern, x)[0]).replace("-", "|")
percent_parser = lambda x: int(re.findall(r"\d+(?=\%$)", x)[0])
score_parser = lambda x: int(str(x).strip().split("/")[0]) if bool(str(x).strip()) else None


class Greatschools_Address(WebText, loader=address_webloader, parsers={"data": address_parser}): pass
class Greatschools_Filtration(WebTexts, loader=filtration_webloader): pass
class Greatschools_Name(WebText, loader=name_webloader, parsers={"data": key_parser}): pass
class Greatschools_Type(WebText, loader=details_webloader, parsers={"data": type_parser}): pass
class Greatschools_Grades(WebText, loader=details_webloader, parsers={"data": grade_parser}): pass
class Greatschools_ScoreKeys(WebTexts, loader=score_keys_webloader, parsers={"data": key_parser}, optional=True): pass
class Greatschools_ScoreValues(WebTexts, loader=score_values_webloader, parsers={"data": score_parser}, optional=True): pass
class Greatschools_TestKeys(WebTexts, loader=test_keys_webloader, parsers={"data": key_parser}, optional=True): pass
class Greatschools_TestValues(WebTexts, loader=test_values_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_TestAverages(WebTexts, loader=test_averages_webloader, parsers={"data": percent_parser}, optional=True): pass
class Greatschools_DemographicKeys(WebTexts, loader=demographic_keys_webloader, parsers={"data": key_parser}, optional=True): pass
class Greatschools_DemographicValues(WebTexts, loader=demographic_values_webloader, parsers={"data": percent_parser}, optional=True): pass


class Greatschools_Scroll(WebScroll): pass
class Greatschools_Schools_WebDelayer(WebDelayer): pass
class Greatschools_Schools_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), headers=Headers(UserAgents.load(USERAGENTS_FILE, limit=100))): pass
class Greatschools_Schools_WebBrowser(WebBrowser, options={"headless": False, "images": True, "incognito": False}): pass


class Greatschools_Schools_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, GID, name, address, **kwargs):
        return [address.state, address.city, "{GID}_{name}".format(GID=str(GID), name="-".join(str(name).split(" ")))]


class Greatschools_Schools_WebQuery(WebQueueable, WebQuery, fields=["GID"]): pass
class Greatschools_Schools_WebDataset(WebDataset, fields=["school", "scores", "testing", "demographics"]): pass


class Greatschools_Schools_WebScheduler(WebScheduler, fields=["GID"]):
    def GID(self, *args, state, city=None, citys=[], zipcode=None, zipcodes=[], **kwargs):
        dataframe = self.load(QUEUE_FILE)
        assert all([isinstance(item, (str, type(None))) for item in (zipcode, city)])
        assert all([isinstance(item, list) for item in (zipcodes, citys)])
        zipcodes = list(set([item for item in [zipcode, *zipcodes] if item]))
        citys = list(set([item for item in [city, *citys] if item]))
        dataframe = dataframe_parser(dataframe, parsers={"address": Address.fromstr}, default=str)
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


class Greatschools_Schools_WebContents(WebPageContents):
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


class Greatschools_Schools_WebPage(WebContentPage, ABC, contents=Greatschools_Schools_WebContents):
    def setup(self, *args, **kwargs):
        try:
            Greatschools_Scroll(self.driver)(*args, commands={"pagedown": 12}, **kwargs)
        except AttributeError:
            pass

    def setup(self, *args, **kwargs): pass
    def query(self): return {"GID": str(identity_parser(self.url))}

    def execute(self, *args, **kwargs):
        query = self.query()
        yield query, "school", self.school()
        yield query, "scores", self.scores()
        yield query, "testing", self.testing()
        yield query, "demographics", self.demographics()

    def school(self):
        school = {"address": self[Greatschools_Schools_WebContents.ADDRESS].data(),
                  "name": self[Greatschools_Schools_WebContents.NAME].data(),
                  "type": self[Greatschools_Schools_WebContents.TYPE].data(),
                  "grade": self[Greatschools_Schools_WebContents.GRADES].data()}
        return {**self.query(), **school}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_Schools_WebContents.SCORE_KEYS]), value=None)
    def scores(self):
        scores = {key.data(): value.data() for key, value in zip(iter(self[Greatschools_Schools_WebContents.SCORE_KEYS]), iter(self[Greatschools_Schools_WebContents.SCORE_VALUES]))}
        return {**self.query(), **scores}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_Schools_WebContents.TEST_KEYS]), value=None)
    def testing(self):
        testing = {key.data(): value.data() for key, value in zip(iter(self[Greatschools_Schools_WebContents.TEST_KEYS]), iter(self[Greatschools_Schools_WebContents.TEST_VALUES]))}
        return {**self.query(), **testing}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_Schools_WebContents.DEMOGRAPHICS_KEYS]), value=None)
    def demographics(self):
        demographics = {key.data(): value.data() for key, value in zip(iter(self[Greatschools_Schools_WebContents.DEMOGRAPHICS_KEYS]), iter(self[Greatschools_Schools_WebContents.DEMOGRAPHICS_VALUES]))}
        return {**self.query(), **demographics}


class Greatschools_Schools_WebDownloader(VPNMixin, CacheMixin, WebDownloader, **__project__):
    def execute(self, *args, browser, reader, queue, delayer, vpn, **kwargs):
        if not bool(queue):
            return
        with browser() as driver:
            with queue:
                for query in iter(queue):
                    with query:
                        if not bool(self.vpn):
                            self.sleep()
                        try:
                            url = self.url(**query.todict())
                            url = Greatschools_Schools_WebURL.fromstr(str(url))
                            page = Greatschools_Schools_WebPage(driver, delayer=delayer)
                            page.load(url, referer="http://www.google.com")
                            page.setup(*args, **kwargs)
                            for fields, dataset, data in page(*args, **kwargs):
                                yield Greatschools_Schools_WebQuery(fields), Greatschools_Schools_WebDataset({dataset: data})
                        except (RefusalError, CaptchaError):
                            self.vpn.trip()
                        except BadRequestError:
                            break

    def url(self, *args, GID, **kwargs):
        dataframe = self.load(QUEUE_FILE)[["GID", "link"]]
        dataframe["GID"] = dataframe["GID"].apply(str)
        dataframe.set_index("GID", inplace=True)
        series = dataframe.squeeze(axis=1)
        return series.get(GID)


def main(*args, **kwargs):
    vpn = NordWebVPN(file=NORDVPN_EXE, server="United States", wait=10)
    delayer = Greatschools_Schools_WebDelayer("random", wait=(30, 120))
    reader = Greatschools_Schools_WebReader(wait=5)
    browser = Greatschools_Schools_WebBrowser(DRIVER_EXE, browser="chrome", loadtime=50, wait=10)
    scheduler = Greatschools_Schools_WebScheduler(*args, file=REPORT_FILE, size=25, **kwargs)
    downloader = Greatschools_Schools_WebDownloader(*args, repository=REPOSITORY_DIR, **kwargs)
    vpn += downloader
    for queue in iter(scheduler)(*args, **kwargs):
        while bool(queue):
            pass

#            vpnthread = Thread(target=vpn, name="NordVPN", daemon=False)
#            thread = Thread(target=downloader, name="Schools", daemon=False, kwargs=dict(browser=browser, queue=queue, delayer=delayer))

            if not bool(downloader):
                break
        if not bool(downloader):
            break
    LOGGER.info(repr(downloader))
    for query, results in downloader.results:
        LOGGER.info(str(query))
        LOGGER.info(str(results))
    if bool(downloader.error):
        raise downloader.error


if __name__ == "__main__":
    sys.argv += ["state=CA", "city=Bakersfield"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)




 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
