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
import traceback
import regex as re
from abc import ABC

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MODULE_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MODULE_DIR, os.pardir))
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
SAVE_DIR = os.path.join(ROOT_DIR, "save")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "schools.csv")
QUEUE_FILE = os.path.join(REPOSITORY_DIR, "links.zip.csv")
USERAGENTS_FILE = os.path.join(RESOURCE_DIR, "useragents.zip.jl")
DRIVER_EXE = os.path.join(RESOURCE_DIR, "chromedriver.exe")
NORDVPN_EXE = os.path.join("C:/", "Program Files", "NordVPN", "NordVPN.exe")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.iostream import InputParser
from utilities.dataframes import dataframe_parser
from utilities.sync import load
from webscraping.webtimers import WebDelayer
from webscraping.webvpn import Nord_WebVPN, WebVPNProcess
from webscraping.webdrivers import WebBrowser
from webscraping.webreaders import WebReader, Retrys, UserAgents, Headers
from webscraping.weburl import WebURL
from webscraping.webpages import WebContentPage, GeneratorMixin, webpage_bypass
from webscraping.webpages import WebData, WebActions, WebConditions
from webscraping.webpages import RefusalError, CaptchaError, BadRequestError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable, WebQueue
from webscraping.webdownloaders import WebDownloader, CacheMixin, DelayMixin
from webscraping.webdata import WebClickable, WebText, WebTexts, WebCaptcha
from webscraping.webactions import WebScroll, WebMoveToClick
from webscraping.webvariables import Address, Price

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Schools_WebDelayer", "Greatschools_Schools_WebBrowser", "Greatschools_Schools_WebReader", "Greatschools_Schools_WebDownloader", "Greatschools_Schools_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


captcha_xpath = r"//*[contains(@class, 'Captcha') or contains(@id, 'Captcha')]"
address_xpath = r"//div[contains(@class, 'address')]//div[@class='content']"
filtration_xpath = r"//div[@class='community-breadcrumbs']//span[not(contains(@class, 'divider')) and not(./span)]"
name_xpath = r"//h1[@class='school-name']"
details_xpath = r"//div[@class='hero-stats-layout']"
score_keys_xpath = r"//div[@class='rating-snapshot']/a//div[contains(@class, 'title')]"
score_values_xpath = r"//div[@class='rating-snapshot']/a//div[contains(@class, 'value')]"
test_keys_xpath = r"//div[@id='Test_scores']//div[contains(@class, 'breakdown')]/span"
test_values_xpath = r"//div[@id='Test_scores']//div[contains(@class, 'percentage')]"
demographic_keys_xpath = r"//div[@id='Students']//div[contains(@class, 'breakdown')]/span"
demographic_values_xpath = r"//div[@id='Students']//div[contains(@class, 'percentage')]"
teacher_keys_xpath = r"//div[@id='Teachers_staff']//div[contains(@class, 'viz-container')]//div[@class='label']"
teacher_values_xpath = r"//div[@id='Teachers_staff']//div[contains(@class, 'viz-container')]//div[@class='text-viz']/div[@class='value']"
teacher_more_xpath = r"//div[@id='Teachers_staff']//div[@class='show-more__button']"


captcha_webloader = WebLoader(xpath=captcha_xpath, timeout=5)
address_webloader = WebLoader(xpath=address_xpath)
filtration_webloader = WebLoader(xpath=filtration_xpath)
name_webloader = WebLoader(xpath=name_xpath)
details_webloader = WebLoader(xpath=details_xpath)
score_keys_webloader = WebLoader(xpath=score_keys_xpath)
score_values_webloader = WebLoader(xpath=score_values_xpath)
test_keys_webloader = WebLoader(xpath=test_keys_xpath)
test_values_webloader = WebLoader(xpath=test_values_xpath)
demographic_keys_webloader = WebLoader(xpath=demographic_keys_xpath)
demographic_values_webloader = WebLoader(xpath=demographic_values_xpath)
teacher_keys_webloader = WebLoader(xpath=teacher_keys_xpath)
teacher_values_webloader = WebLoader(xpath=teacher_values_xpath)
teacher_more_webloader = WebLoader(xpath=teacher_more_xpath)


identity_pattern = "(?<=\/)\d+"
type_pattern = "[A-Za-z]+(?= school)"
grade_pattern = "(?<=Grades |Grade )[0-9A-Z-]+"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
address_parser = lambda x: str(Address.fromsearch(x))
price_parser = lambda x: str(Price.fromsearch(x))
str_parser = lambda x: str(x).strip()
type_parser = lambda x: str(re.findall(type_pattern, x)[0]).lower().title()
grade_parser = lambda x: str(re.findall(grade_pattern, x)[0]).replace("-", "|")


class Greatschools_Captcha(WebCaptcha, loader=captcha_webloader, optional=True): pass
class Greatschools_Address(WebText, loader=address_webloader, parsers={"data": address_parser}): pass
class Greatschools_Filtration(WebTexts, loader=filtration_webloader): pass
class Greatschools_Name(WebText, loader=name_webloader, parsers={"data": str_parser}): pass
class Greatschools_Type(WebText, loader=details_webloader, parsers={"data": type_parser}): pass
class Greatschools_Grades(WebText, loader=details_webloader, parsers={"data": grade_parser}): pass
class Greatschools_ScoreKeys(WebTexts, loader=score_keys_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_ScoreValues(WebTexts, loader=score_values_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_TestKeys(WebTexts, loader=test_keys_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_TestValues(WebTexts, loader=test_values_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_DemographicKeys(WebTexts, loader=demographic_keys_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_DemographicValues(WebTexts, loader=demographic_values_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_TeacherKeys(WebTexts, loader=teacher_keys_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_TeacherValues(WebTexts, loader=teacher_values_webloader, parsers={"data": str_parser}, optional=True): pass
class Greatschools_TeacherMore(WebClickable, loader=teacher_more_webloader, optional=True): pass


class Greatschools_Scroll(WebScroll, wait=3): pass
class Greatschools_TeacherMore_MoveToClick(WebMoveToClick, on=Greatschools_TeacherMore): pass


class Greatschools_Schools_WebDelayer(WebDelayer): pass
class Greatschools_Schools_WebBrowser(WebBrowser, files={"chrome": DRIVER_EXE}, options={"headless": False, "images": True, "incognito": False}): pass
class Greatschools_Schools_WebReader(WebReader, retrys=Retrys(retries=3, backoff=0.3, httpcodes=(500, 502, 504)), headers=Headers(UserAgents.load(USERAGENTS_FILE, limit=100))): pass


class Greatschools_Schools_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, GID, name, address, **kwargs):
        return [address.state, address.city, "{GID}_{name}".format(GID=str(GID), name="-".join(str(name).split(" ")))]


class Greatschools_Schools_WebQueue(WebQueue): pass
class Greatschools_Schools_WebQuery(WebQuery, WebQueueable, fields=["GID"]): pass
class Greatschools_Schools_WebDataset(WebDataset, fields=["schools", "scores", "testing", "demographics", "teachers"]): pass


class Greatschools_Schools_WebScheduler(WebScheduler, fields=["GID"]):
    @staticmethod
    def GID(*args, state, city=None, citys=[], zipcode=None, zipcodes=[], **kwargs):
        try:
            dataframe = load(QUEUE_FILE)
        except FileNotFoundError:
            return []
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
    def execute(querys, *args, **kwargs):
        queueables = [Greatschools_Schools_WebQuery(query, name="GreatSchoolsQuery") for query in querys]
        queue = Greatschools_Schools_WebQueue(queueables, *args, name="GreatSchoolsQueue", **kwargs)
        return queue


class Greatschools_WebData(WebData):
    FILTRATION = Greatschools_Filtration
    ADDRESS = Greatschools_Address
    NAME = Greatschools_Name
    TYPE = Greatschools_Type
    GRADES = Greatschools_Grades


class Greatschools_WebScore(WebData):
    KEYS = Greatschools_ScoreKeys
    VALUES = Greatschools_ScoreValues


class Greatschools_WebTest(WebData):
    KEYS = Greatschools_TestKeys
    VALUES = Greatschools_TestValues


class Greatschools_WebDemographic(WebData):
    KEYS = Greatschools_DemographicKeys
    VALUES = Greatschools_DemographicValues


class Greatschools_WebTeacher(WebData):
    KEYS = Greatschools_TeacherKeys
    VALUES = Greatschools_TeacherValues


class Greatschools_WebActions(WebActions):
    SCROLL = Greatschools_Scroll
    OPEN = Greatschools_TeacherMore_MoveToClick


class Greatschools_WebConditions(WebConditions):
    CAPTCHA = Greatschools_Captcha


contents = [Greatschools_WebData, Greatschools_WebScore, Greatschools_WebTest, Greatschools_WebDemographic, Greatschools_WebTeacher, Greatschools_WebActions, Greatschools_WebConditions]


class Greatschools_Schools_WebPage(GeneratorMixin, WebContentPage, ABC, contents=contents):
    def query(self): return {"GID": str(identity_parser(self.url))}

    def setup(self, *args, **kwargs):
        if not hasattr(self, "driver"):
            return
        self[Greatschools_WebActions.SCROLL](*args, commands={"pagedown": 20}, **kwargs)
        if bool(self[Greatschools_WebActions.OPEN]):
            self[Greatschools_WebActions.OPEN](*args, **kwargs)

    def execute(self, *args, **kwargs):
        query = self.query()
        yield query, "schools", self.schools()
        yield query, "scores", self.scores()
        yield query, "testing", self.testing()
        yield query, "demographics", self.demographics()
        yield query, "teachers", self.teachers()

    def schools(self):
        schools = {}
        if bool(self[Greatschools_WebData.ADDRESS]):
            schools["address"] = str(self[Greatschools_WebData.ADDRESS].data())
        if bool(self[Greatschools_WebData.NAME]):
            schools["name"] = str(self[Greatschools_WebData.NAME].data())
        if bool(self[Greatschools_WebData.TYPE]):
            schools["type"] = str(self[Greatschools_WebData.TYPE].data())
        if bool(self[Greatschools_WebData.GRADES]):
            schools["grades"] = str(self[Greatschools_WebData.GRADES].data())
        return {**self.query(), **schools}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_WebScore.KEYS]), value=None)
    def scores(self):
        items = zip(iter(self[Greatschools_WebScore.KEYS]), iter(self[Greatschools_WebScore.VALUES]))
        scores = {key.data(): value.data() for key, value in items}
        return {**self.query(), **scores}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_WebTest.KEYS]), value=None)
    def testing(self):
        items = zip(iter(self[Greatschools_WebTest.KEYS]), iter(self[Greatschools_WebTest.VALUES]))
        testing = {key.data(): value.data() for key, value in items}
        return {**self.query(), **testing}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_WebDemographic.KEYS]), value=None)
    def demographics(self):
        items = zip(iter(self[Greatschools_WebDemographic.KEYS]), iter(self[Greatschools_WebDemographic.VALUES]))
        demographics = {key.data(): value.data() for key, value in items}
        return {**self.query(), **demographics}

    @webpage_bypass(condition=lambda self: not bool(self[Greatschools_WebTeacher.KEYS]), value=None)
    def teachers(self):
        items = zip(iter(self[Greatschools_WebTeacher.KEYS]), iter(self[Greatschools_WebTeacher.VALUES]))
        demographics = {key.data(): value.data() for key, value in items}
        return {**self.query(), **demographics}


class Greatschools_Schools_WebDownloader(DelayMixin, CacheMixin, WebVPNProcess, WebDownloader):
    def execute(self, *args, browser, scheduler, delayer, referer="https://www.google.com", **kwargs):
        with scheduler(*args, **kwargs) as queue:
            if not queue:
                self.delay()
            with browser() as driver:
                page = Greatschools_Schools_WebPage(driver, name="GreatSchoolsPage", delayer=delayer)
                with queue:
                    for query in queue:
                        if not bool(self.vpn):
                            self.wait()
                        if not bool(driver):
                            driver.reset()
                        url = self.url(**query.todict())
                        url = Greatschools_Schools_WebURL.fromstr(str(url))
                        try:
                            page.load(str(url), referer=referer)
                            page.setup(*args, **kwargs)
                            for fields, dataset, data in page(*args, **kwargs):
                                yield Greatschools_Schools_WebQuery(fields, name="GreatschoolsQuery"), Greatschools_Schools_WebDataset({dataset: data}, name="GreatschoolsDataset")
                        except (RefusalError, CaptchaError):
                            driver.trip()
                            self.trip()
                            query.abandon()
                        except BadRequestError:
                            query.success()
                        except BaseException as error:
                            query.error()
                            raise error
                        else:
                            query.success()

    @staticmethod
    def url(*args, GID, **kwargs):
        urls = load(QUEUE_FILE)[["GID", "link"]]
        urls["GID"] = urls["GID"].apply(str)
        urls.set_index("GID", inplace=True)
        series = urls.squeeze(axis=1)
        url = series.get(GID)
        return url


def main(*args, **kwargs):
    delayer = Greatschools_Schools_WebDelayer(name="GreatSchoolsDelayer", method="random", wait=(30, 60))
    reader = Greatschools_Schools_WebReader(name="GreatSchoolsReader", wait=10)
    browser = Greatschools_Schools_WebBrowser(name="GreatSchoolsBrowser", browser="chrome", loadtime=50, wait=10)
    scheduler = Greatschools_Schools_WebScheduler(name="GreatSchoolsScheduler", randomize=True, size=1, file=REPORT_FILE)
    downloader = Greatschools_Schools_WebDownloader(name="GreatSchools", repository=REPOSITORY_DIR, timeout=60*2, delay=None)
    vpn = Nord_WebVPN(name="NordVPN", file=NORDVPN_EXE, server="United States", loadtime=10, wait=10, timeout=60*2)
    vpn += downloader
    downloader(*args, browser=browser, reader=reader, scheduler=scheduler, delayer=delayer, **kwargs)
    vpn.start()
    vpn.join()
    for query, results in downloader.results:
        LOGGER.info(str(query))
        LOGGER.info(str(results))
    if bool(vpn.error):
        traceback.print_exception(*vpn.error)
    if bool(downloader.error):
        traceback.print_exception(*downloader.error)


if __name__ == "__main__":
    sys.argv += ["state=CA", "city=Bakersfield"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)




 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
