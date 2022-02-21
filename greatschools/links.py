# -*- coding: utf-8 -*-
"""
Created on Sun May 2 2021
@name:   Greatschools Links Download Application
@author: Jack Kirby Cook

"""

import sys
import os.path
import warnings
import logging
import traceback
import regex as re

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MODULE_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MODULE_DIR, os.pardir))
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
SAVE_DIR = os.path.join(ROOT_DIR, "save")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "links.csv")
QUEUE_FILE = os.path.join(RESOURCE_DIR, "zipcodes.zip.csv")
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
from webscraping.webpages import WebBrowserPage, IterationMixin, PaginationMixin, GeneratorMixin, ContentMixin
from webscraping.webpages import WebData, WebConditions, WebOperations
from webscraping.webpages import RefusalError, CaptchaError, BadRequestError, PaginationError, StaleError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable, WebQueue
from webscraping.webdownloaders import WebDownloader, CacheMixin
from webscraping.webdata import WebClickable, WebText, WebLink, WebClickables, WebBadRequest, WebCaptcha
from webscraping.webactions import WebMoveToClick
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Links_WebDelayer", "Greatschools_Links_WebBrowser", "Greatschools_Links_WebDownloader", "Greatschools_Links_WebScheduler"]
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


captcha_webloader = WebLoader(xpath=captcha_xpath, timeout=5)
badrequest_webloader = WebLoader(xpath=badrequest_xpath)
zipcode_webloader = WebLoader(xpath=zipcode_xpath)
results_webloader = WebLoader(xpath=results_xpath)
contents_webloader = WebLoader(xpath=contents_xpath)
link_contents_webloader = WebLoader(xpath=link_contents_xpath)
address_contents_webloader = WebLoader(xpath=address_contents_xpath)
name_contents_webloader = WebLoader(xpath=name_contents_xpath)
type_contents_webloader = WebLoader(xpath=type_contents_xpath)
previous_webloader = WebLoader(xpath=previous_xpath)
nextpage_webloader = WebLoader(xpath=nextpage_xpath)
current_webloader = WebLoader(xpath=current_xpath)
pagination_webloader = WebLoader(xpath=pagination_xpath)


identity_pattern = "(?<=\/)\d+(?=\-)"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
zipcode_parser = lambda x: str(re.findall(r"\d{5}$", x)[0])
results_parser = lambda x: str(re.findall(r"(?<=of )[\d\,]+(?= schools)", x)[0])
address_parser = lambda x: Address.fromsearch(x)
name_parser = lambda x: str(x).strip()
type_parser = lambda x: str(re.findall(re.compile(r"Public district|Public charter|Private", re.IGNORECASE), x)[0])
pagination_parser = lambda x: str(int(str(x).strip()))


class Greatschools_Captcha(WebCaptcha, loader=captcha_webloader, optional=True): pass
class Greatschools_BadRequest(WebBadRequest, loader=badrequest_webloader, optional=True): pass
class Greatschools_Zipcode(WebText, loader=zipcode_webloader, parsers={"data": zipcode_parser}): pass
class Greatschools_Results(WebText, loader=results_webloader, parsers={"data": results_parser}, optional=True): pass
class Greatschools_Contents(WebClickables, loader=contents_webloader, optional=True): pass
class Greatschools_ContentsLink(WebLink, loader=link_contents_webloader, parsers={"data": identity_parser}): pass
class Greatschools_ContentsAddress(WebText, loader=address_contents_webloader, parsers={"data": address_parser}, optional=True): pass
class Greatschools_ContentsName(WebText, loader=name_contents_webloader, parsers={"data": name_parser}, optional=True): pass
class Greatschools_ContentsType(WebText, loader=type_contents_webloader, parsers={"data": type_parser}, optional=True): pass
class Greatschools_Current(WebText, loader=current_webloader, parsers={"data": pagination_parser}, optional=True): pass
class Greatschools_Previous(WebClickable, loader=previous_webloader, optional=True): pass
class Greatschools_NextPage(WebClickable, loader=nextpage_webloader, optional=True): pass
class Greatschools_Pagination(WebClickable, loader=pagination_webloader, parsers={"key": pagination_parser}, optional=True): pass


Greatschools_Contents["link"] = Greatschools_ContentsLink
Greatschools_Contents["address"] = Greatschools_ContentsAddress
Greatschools_Contents["name"] = Greatschools_ContentsName
Greatschools_Contents["type"] = Greatschools_ContentsType


class Greatschools_Previous_MoveToClick(WebMoveToClick, on=Greatschools_Previous): pass
class Greatschools_NextPage_MoveToClick(WebMoveToClick, on=Greatschools_NextPage): pass
class Greatschools_Pagination_MoveToClick(WebMoveToClick, on=Greatschools_Pagination): pass


class Greatschools_Links_WebDelayer(WebDelayer): pass
class Greatschools_Links_WebBrowser(WebBrowser, files={"chrome": DRIVER_EXE}, options={"headless": False, "images": True, "incognito": False}): pass


class Greatschools_Links_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, **kwargs): return ["search", "search.zipcode"]
    @staticmethod
    def parm(*args, zipcode, pagination=1, **kwargs): return {"page": str(int(pagination)) if pagination > 1 else None, "sort": "rating", "zip": "{:05.0f}".format(int(zipcode))}


class Greatschools_Links_WebQueue(WebQueue): pass
class Greatschools_Links_WebQuery(WebQuery, WebQueueable, fields=["dataset", "zipcode"]): pass
class Greatschools_Links_WebDataset(WebDataset, fields=["links"]): pass


class Greatschools_Links_WebScheduler(WebScheduler, fields=["dataset", "zipcode"], dataset=["school"]):
    @staticmethod
    def zipcode(*args, state, county=None, countys=[], city=None, citys=[], **kwargs):
        dataframe = load(QUEUE_FILE)
        assert all([isinstance(item, (str, type(None))) for item in (county, city)])
        assert all([isinstance(item, list) for item in (countys, citys)])
        countys = list(set([item for item in [county, *countys] if item]))
        citys = list(set([item for item in [city, *citys] if item]))
        dataframe = dataframe_parser(dataframe, parsers={"zipcode": lambda x: "{:05.0f}".format(int(x))}, default=str)
        dataframe = dataframe[["zipcode", "type", "city", "state", "county"]]
        dataframe = dataframe[dataframe["type"] == "standard"][["zipcode", "city", "state", "county"]].reset_index(drop=True)
        if citys or countys:
            dataframe = dataframe[(dataframe["city"].isin(list(citys)) | dataframe["county"].isin(list(countys)))]
        if state:
            dataframe = dataframe[dataframe["state"] == state]
        return list(dataframe["zipcode"].to_numpy())

    @staticmethod
    def execute(querys, *args, **kwargs):
        queueables = [Greatschools_Links_WebQuery(query, name="GreatSchoolsQuery") for query in querys]
        queue = Greatschools_Links_WebQueue(queueables, *args, name="GreatSchoolsQueue", **kwargs)
        return queue


class Greatschools_WebData(WebData):
    ZIPCODE = Greatschools_Zipcode
    RESULTS = Greatschools_Results


class Greatschools_WebConditions(WebConditions):
    CAPTCHA = Greatschools_Captcha
    BADREQUEST = Greatschools_BadRequest


class Greatschools_WebOperations(WebOperations):
    ITERATOR = Greatschools_Contents
    NEXT = Greatschools_NextPage_MoveToClick


class Greatschools_Links_WebPage(IterationMixin, PaginationMixin, GeneratorMixin, ContentMixin, WebBrowserPage, contents=[Greatschools_WebData, Greatschools_WebConditions, Greatschools_WebOperations]):
    def query(self): return {"dataset": "school", "zipcode": str(self[Greatschools_WebData.ZIPCODE].data())}
    def setup(self, *args, **kwargs): pass

    def execute(self, *args, **kwargs):
        if not bool(self[Greatschools_WebData.RESULTS]):
            return
        query = self.query()
        data = [{"GID": content["link"].data(), "address": content["address"].data(), "link": content["link"].url} for content in iter(self)]
        yield query, "links", data
        nextpage = next(self)
        if bool(nextpage):
            nextpage.setup(*args, **kwargs)
            yield from nextpage(*args, **kwargs)
        else:
            return


class Greatschools_Links_WebDownloader(CacheMixin, WebVPNProcess, WebDownloader, basis="GID"):
    def execute(self, *args, browser, scheduler, delayer, referer="https://www.google.com", **kwargs):
        with scheduler(*args, **kwargs) as queue:
            if not queue:
                return
            with browser() as driver:
                page = Greatschools_Links_WebPage(driver, name="GreatSchoolsPage", delayer=delayer)
                with queue:
                    for query in queue:
                        url = Greatschools_Links_WebURL(**query.todict())
                        reload = False
                        while True:
                            if not bool(self.vpn):
                                self.wait()
                            if not bool(driver):
                                driver.reset()
                            try:
                                page.reload(referer=referer) if reload else page.load(str(url), referer=referer)
                                page.setup(*args, **kwargs)
                                for fields, dataset, data in page(*args, **kwargs):
                                    yield Greatschools_Links_WebQuery(fields, name="GreatSchoolsQuery"), Greatschools_Links_WebDataset({dataset: data}, name="GreatSchoolsDataset")
                            except (RefusalError, CaptchaError):
                                driver.trip()
                                self.trip()
                                reload = True
                            except BadRequestError:
                                query.success()
                                break
                            except (PaginationError, StaleError):
                                query.failure()
                                break
                            except BaseException as error:
                                query.error()
                                raise error
                            else:
                                query.success()
                                break


def main(*args, **kwargs):
    delayer = Greatschools_Links_WebDelayer(name="GreatSchoolsDelayer", method="random", wait=(10, 20))
    browser = Greatschools_Links_WebBrowser(name="GreatSchoolsBrowser", browser="chrome", loadtime=50, wait=10)
    scheduler = Greatschools_Links_WebScheduler(name="GreatSchoolsScheduler", randomize=True, size=10, file=REPORT_FILE)
    downloader = Greatschools_Links_WebDownloader(name="GreatSchools", repository=REPOSITORY_DIR, timeout=60*2)
    vpn = Nord_WebVPN(name="NordVPN", file=NORDVPN_EXE, server="United States", loadtime=10, wait=10, timeout=60*2)
    vpn += downloader
    downloader(*args, browser=browser, scheduler=scheduler, delayer=delayer, **kwargs)
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
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)

















