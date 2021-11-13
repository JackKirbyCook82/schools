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
import regex as re

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MOD_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MOD_DIR, os.pardir))
SAVE_DIR = os.path.join(ROOT_DIR, "save")
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
DRIVER_EXE = os.path.join(RESOURCE_DIR, "chromedriver.exe")
NORDVPN_EXE = os.path.join("C:/", "Program Files", "NordVPN", "NordVPN.exe")
QUEUE_FILE = os.path.join(RESOURCE_DIR, "zipcodes.zip.csv")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "links.csv")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webtimers import WebDelayer
from webscraping.webvpn import NordVPN
from webscraping.webdrivers import WebDriver
from webscraping.weburl import WebURL
from webscraping.webpages import WebBrowserPage, IterationMixin, PaginationMixin, CrawlingMixin, WebDatas, WebActions, BadRequestError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable
from webscraping.webdownloaders import WebDownloader, CacheMixin
from webscraping.webdata import WebClickable, WebText, WebLink, WebBadRequest, WebCaptcha, WebClickables
from webscraping.webactions import WebMoveToClick, WebClearCaptcha
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Links_WebDelayer", "Greatschools_Links_WebDownloader", "Greatschools_Links_WebScheduler"]
__copyright__ = "Copyright 2021, Jack Kirby Cook"
__license__ = ""
__project__ = {"website": "GreatSchools", "project": "Links"}


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


class Greatschools_Captcha_ClearCaptcha(WebClearCaptcha, on=Greatschools_Captcha, wait=60*5): pass
class Greatschools_Previous_MoveToClick(WebMoveToClick, on=Greatschools_Previous): pass
class Greatschools_NextPage_MoveToClick(WebMoveToClick, on=Greatschools_NextPage): pass
class Greatschools_Pagination_MoveToClick(WebMoveToClick, on=Greatschools_Pagination): pass


class Greatschools_Links_WebDelayer(WebDelayer): pass
class Greatschools_Links_WebDriver(WebDriver, options={"headless": False, "images": True, "incognito": False}): pass


class Greatschools_Links_WebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, **kwargs):
        if "zipcode" in kwargs.keys():
            return ["search", "search.zipcode"]
        elif "city" in kwargs.keys() and "state" in kwargs.keys():
            return ["search", "search.page"]
        else:
            raise ValueError(list(kwargs.keys()))

    @staticmethod
    def parm(*args, **kwargs):
        if "zipcode" in kwargs.keys():
            return {"zip": "{:05.0f}".format(int(kwargs["zipcode"]))}
        else:
            return {"q": "%20".join([item.replace(" ", "-").lower() for item in (kwargs["city"], kwargs["state"])])}


class Greatschools_Links_WebQuery(WebQuery, WebQueueable, fields=["dataset", "zipcode"], **__project__): pass
class Greatschools_Links_WebDataset(WebDataset, fields=["links"], **__project__): pass


class Greatschools_Links_WebScheduler(WebScheduler, fields=["dataset", "zipcode"], dataset=["school"], **__project__):
    def zipcode(self, *args, state, county=None, countys=[], city=None, citys=[], **kwargs):
        dataframe = self.load(QUEUE_FILE)
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
    def execute(querys, *args, **kwargs): return [Greatschools_Links_WebQuery(query) for query in querys]


class Greatschools_Links_WebDatas(WebDatas):
    ZIPCODE = Greatschools_Zipcode
    RESULTS = Greatschools_Results
    BADREQUEST = Greatschools_BadRequest
    ITERATOR = Greatschools_Contents
    NEXT = Greatschools_NextPage


class Greatschools_Links_WebActions(WebActions):
    CAPTCHA = Greatschools_Captcha_ClearCaptcha
    NEXT = Greatschools_NextPage_MoveToClick


class Greatschools_Links_WebPage(CrawlingMixin, IterationMixin, PaginationMixin, WebBrowserPage, datas=Greatschools_Links_WebDatas, actions=Greatschools_Links_WebActions):
    def setup(self, *args, **kwargs):
        if not bool(self[Greatschools_Links_WebDatas.ZIPCODE]):
            pass

    def execute(self, *args, **kwargs):
        if not bool(self[Greatschools_Links_WebDatas.RESULTS]):
            return
        query = self.query()
        for content in iter(self):
            data = {"GID": content["link"].data(), "address": content["address"].data(), "link": content["link"].url}
            yield query, "links", data
        nextpage = next(self)
        if bool(nextpage):
            nextpage.setup(*args, **kwargs)
            yield from nextpage(*args, **kwargs)
        else:
            return

    def query(self):
        return {"dataset": "school", "zipcode": str(self[Greatschools_Links_WebDatas.ZIPCODE].data())}
 

class Greatschools_Links_WebDownloader(CacheMixin, WebDownloader, basis="GID", **__project__):
    @staticmethod
    def execute(*args, queue, delayer, **kwargs):
        if not queue:
            return
        with Greatschools_Links_WebDriver(DRIVER_EXE, browser="chrome", loadtime=50) as driver:
            page = Greatschools_Links_WebPage(driver, delayer=delayer)
            with queue:
                for query in iter(queue):
                    with query:
                        url = Greatschools_Links_WebURL(**query.todict())
                        try:
                            page.load(url, referer="http://www.google.com")
                        except BadRequestError:
                            yield query, Greatschools_Links_WebDataset({})
                            return
                        page.setup(*args, **kwargs)
                        for fields, dataset, data in page(*args, **kwargs):
                            yield Greatschools_Links_WebQuery(fields), Greatschools_Links_WebDataset({dataset: data})

    
def main(*args, proxylimit=5, **kwargs):
    delayer = Greatschools_Links_WebDelayer("random", wait=(10, 20))
    vpn = NordVPN(NORDVPN_EXE, server="United States", wait=10)
    scheduler = Greatschools_Links_WebScheduler(*args, file=REPORT_FILE, **kwargs)
    downloader = Greatschools_Links_WebDownloader(*args, repository=REPOSITORY_DIR, **kwargs)
    queue = scheduler(*args, **kwargs)
    while bool(queue):
        with vpn:
            downloader(*args, queue=queue.export(proxylimit), delayer=delayer, **kwargs)
            if not bool(downloader):
                break
    LOGGER.info(str(downloader))
    for query, results in downloader.results:
        LOGGER.info(str(query))
        LOGGER.info(str(results))
    if not bool(downloader):
        raise downloader.error


if __name__ == "__main__":
    sys.argv += ["state=CA", "city=Bakersfield"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)

















