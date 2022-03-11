# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Boundary Download Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import warnings
import logging
import traceback
import regex as re
from abc import ABC
from datetime import date as Date

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MODULE_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MODULE_DIR, os.pardir))
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
SAVE_DIR = os.path.join(ROOT_DIR, "save")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "boundary.csv")
QUEUE_FILE = os.path.join(REPOSITORY_DIR, "boundary.zip.csv")
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
from webscraping.weburl import WebURL
from webscraping.webpages import WebBrowserPage, ContentMixin
from webscraping.webpages import WebConditions
from webscraping.weberrors import WebPageError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable, WebQueue
from webscraping.webdownloaders import WebDownloader, CacheMixin
from webscraping.webdata import WebCaptcha
from webscraping.webactions import StaleWebActionError, InteractionWebActionError
from webscraping.webvariables import Address


__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Boundary_WebDelayer", "Greatschools_Boundary_WebBrowser", "Greatschools_Boundary_WebDownloader", "Greatschools_Boundary_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""

LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

captcha_xpath = r"//*[contains(@class, 'Captcha') or contains(@id, 'Captcha')]"
address_xpath = r"//div[contains(@class, 'address')]//div[@class='content']"
filtration_xpath = r"//div[@class='community-breadcrumbs']//span[not(contains(@class, 'divider')) and not(./span)]"
name_xpath = r"//h1[@class='school-name']"
details_xpath = r"//div[@class='hero-stats-layout']"
boundary_xpath = r"//div[@data-ga-click-label='Neighborhood']//a[text()='School attendance zone']"
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
identity_pattern = "(?<=\/)\d+"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
link_parser = lambda x: "".join(["https://www.greatschools.org", x]) if not str(x).startswith("https://www.greatschools.org") else x


class Greatschools_Captcha(WebCaptcha, loader=captcha_webloader, optional=True): pass


class Greatschools_Boundary_WebDelayer(WebDelayer): pass
class Greatschools_Boundary_WebBrowser(WebBrowser, files={"chrome": DRIVER_EXE}, options={"headless": False, "images": True, "incognito": False}): pass


class Greatschools_Boundary_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, GID, name, address, **kwargs): return [address.state, address.city, "{GID}_{name}".format(GID=str(GID), name="-".join(str(name).split(" ")))]


# class Greatschools_BoundaryShape_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
#    @staticmethod
#    def path(*args, **kwargs): pass
#    @staticmethod
#    def parm(*args, **kwargs): pass


class Greatschools_Boundary_WebQueue(WebQueue): pass
class Greatschools_Boundary_WebQuery(WebQuery, WebQueueable, fields=["GID"]): pass
class Greatschools_Boundary_WebDataset(WebDataset, fields=["boundary"]): pass


class Greatschools_Boundary_WebScheduler(WebScheduler, fields=["GID"]):
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
        dataframe = dataframe.drop_duplicates(subset="GID", keep="last", ignore_index=True)
        return list(dataframe["GID"].to_numpy())

    @staticmethod
    def execute(querys, *args, **kwargs):
        queueables = [Greatschools_Boundary_WebQuery(query, name="GreatSchoolsQuery") for query in querys]
        queue = Greatschools_Boundary_WebQueue(queueables, *args, name="GreatSchoolsQueue", **kwargs)
        return queue


class Greatschools_WebConditions(WebConditions):
    CAPTCHA = Greatschools_Captcha


class Greatschools_Boundary_WebPage(ContentMixin, WebBrowserPage, ABC, contents=[Greatschools_WebConditions]):
    @staticmethod
    def date(): return {"date": Date.today().strftime("%m/%d/%Y")}
    def query(self): return {"GID": str(identity_parser(self.url))}
    def setup(self, *args, **kwargs): pass

#    def execute(self, *args, **kwargs):
#        pass


class Greatschools_Boundary_WebDownloader(CacheMixin, WebVPNProcess, WebDownloader):
    def execute(self, *args, browser, scheduler, delayer, referer="https://www.google.com", **kwargs):
        with scheduler(*args, **kwargs) as queue:
            if not queue:
                return
            with browser() as driver:
                page = Greatschools_Boundary_WebPage(driver, name="GreatSchoolsPage", delayer=delayer)
                with queue:
                    for query in queue:
                        if not bool(self.vpn.operational):
                            query.abandon()
                            self.terminate()
                        elif not bool(self.vpn.ready):
                            ready = self.wait()
                            if not ready:
                                query.abandon()
                                self.terminate()
                        if not bool(driver):
                            driver.reset()
                        url = self.url(**query.todict())
                        url = Greatschools_Boundary_WebURL.fromstr(str(url))
                        try:
                            page.load(str(url), referer=referer)
                            page.setup(*args, **kwargs)
                            for fields, dataset, data in page(*args, **kwargs):
                                yield Greatschools_Boundary_WebQuery(fields, name="GreatschoolsQuery"), Greatschools_Boundary_WebDataset({dataset: data}, name="GreatschoolsDataset")
                        except (WebPageError["refusal"], WebPageError["captcha"]):
                            driver.trip()
                            self.vpn.trip()
                            query.abandon()
                        except WebPageError["badrequest"]:
                            query.success()
                        except (StaleWebActionError, InteractionWebActionError):
                            query.failure()
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
    delayer = Greatschools_Boundary_WebDelayer(name="GreatSchoolsDelayer", method="random", wait=(30, 60))
    browser = Greatschools_Boundary_WebBrowser(name="GreatSchoolsBrowser", browser="chrome", timeout=60, wait=15)
    scheduler = Greatschools_Boundary_WebScheduler(name="GreatSchoolsScheduler", randomize=True, size=50,file=REPORT_FILE)
    downloader = Greatschools_Boundary_WebDownloader(name="GreatSchools", repository=REPOSITORY_DIR, timeout=60*2, wait=15)
    vpn = Nord_WebVPN(name="NordVPN", file=NORDVPN_EXE, server="United States", timeout=60 * 2, wait=15)
    vpn += downloader
    downloader(*args, browser=browser, scheduler=scheduler, delayer=delayer, **kwargs)
    vpn.start()
    downloader.start()
    downloader.join()
    vpn.stop()
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
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)














