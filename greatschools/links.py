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
DRIVER_FILE = os.path.join(RESOURCE_DIR, "chromedriver.exe")
QUEUE_FILE = os.path.join(RESOURCE_DIR, "zipcodes.zip.csv")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "links.csv")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.input import InputParser
from utilities.dataframes import dataframe_parser
from webscraping.webtimers import WebDelayer
from webscraping.webdrivers import WebDriver
from webscraping.weburl import WebURL
from webscraping.webpages import WebBrowserPage, IterationMixin, PaginationMixin, CrawlingMixin
from webscraping.webpages import WebContents, BadRequestError, MulliganError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDatasets
from webscraping.webqueues import WebScheduler
from webscraping.webdownloaders import WebDownloader, CacheMixin, AttemptsMixin
from webscraping.webdata import WebClickable, WebKeyedClickable, WebText, WebLink, WebKeyedLink, WebBadRequest, WebCaptcha
from webscraping.webactions import WebMoveToClick, WebClearCaptcha
from webscraping.webvariables import Address

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Greatschools_Links_WebDelayer", "Greatschools_Links_WebDownloader", "Greatschools_Links_WebScheduler"]
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


identity_parser = lambda x: x.replace("https://www.greatschools.org", "")
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
class Greatschools_Contents(WebClickable, loader=contents_webloader, optional=True): pass
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
class Greatschools_Pagination_MoveToClick(WebMoveToClick, on=Greatschools_Pagination,): pass


class Greatschools_Links_WebDelayer(WebDelayer): pass
class Greatschools_Links_WebDriver(WebDriver, options={"headless": False, "images": True, "incognito": False}): pass


class Greatschools_Links_WebURL(WebURL, protocol="https", domain="www.greatschools.org", separator="%20", spaceproxy="-"):
    def path(self, *args, **kwargs):
        if "zipcode" in kwargs.keys():
            return ["search", "search.zipcode"]
        elif "city" in kwargs.keys() and "state" in kwargs.keys():
            return ["search", "search.page"]
        else:
            raise ValueError(list(kwargs.keys()))

    def parm(self, *args, **kwargs):
        if "zipcode" in kwargs.keys():
            return {"zip": "{:05.0f}".format(int(kwargs["zipcode"]))}
        else:
            return {"q": self.separator.join(
                [item.replace(" ", self.spaceproxy).lower() for item in (kwargs["city"], kwargs["state"])])}


class Greatschools_Links_WebQuery(WebQuery, fields=["dataset", "zipcode"]): pass
class Greatschools_Links_WebDatasets(WebDatasets, feilds=["links"]): pass


class Greatschools_Links_WebScheduler(WebScheduler, fields=["dataset", "zipcode"], dataset=["school"]):
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


class Greatschools_Links_WebContents(WebContents):
    ZIPCODE = Greatschools_Zipcode
    RESULTS = Greatschools_Results


Greatschools_Links_WebContents.CAPTCHA += Greatschools_Captcha_ClearCaptcha
Greatschools_Links_WebContents.BADREQUEST += Greatschools_BadRequest
Greatschools_Links_WebContents.ITERATOR += Greatschools_Contents
Greatschools_Links_WebContents.PREVIOUS += Greatschools_Previous_MoveToClick
Greatschools_Links_WebContents.NEXT += Greatschools_NextPage_MoveToClick
Greatschools_Links_WebContents.CURRENT += Greatschools_Current
Greatschools_Links_WebContents.PAGINATION += Greatschools_Pagination_MoveToClick


class Greatschools_Links_WebPage(WebBrowserPage + (IterationMixin, PaginationMixin, CrawlingMixin), contents=Greatschools_Links_WebContents):
    def setup(self, *args, **kwargs):
        self.load[Greatschools_Links_WebContents.ZIPCODE](*args, **kwargs)
        self.load[Greatschools_Links_WebContents.RESULTS](*args, **kwargs)
        if not bool(self[Greatschools_Links_WebContents.ZIPCODE]):
            raise MulliganError(str(self))

    @property
    def query(self): return {"dataset": "school", "zipcode": str(self[Greatschools_Links_WebContents.ZIPCODE].data())}

    def execute(self, *args, **kwargs):
        if not bool(self[Greatschools_Links_WebContents.RESULTS]):
            return
        for content in iter(self):
            data = {"GID": content["link"].data(), "address": content["address"].data(), "link": content["link"].url}
            yield "links", data
        nextpage = next(self)
        if bool(nextpage):
            nextpage.setup(*args, **kwargs)
            yield from nextpage(*args, **kwargs)
        else:
            return
 

class Greatschools_Links_WebDownloader(WebDownloader + (CacheMixin, AttemptsMixin), basis="GID", attempts=3, delay=30):
    @staticmethod
    def execute(*args, queue, delayer, **kwargs):
        with Greatschools_Links_WebDriver(DRIVER_FILE, browser="chrome", loadtime=50) as driver:
            page = Greatschools_Links_WebPage(driver, delayer=delayer)
            for query in iter(queue):
                with query:
                    url = Greatschools_Links_WebURL(**query.todict())
                    try:
                        page.load(url, referer=None)
                    except BadRequestError:
                        yield query, Greatschools_Links_WebDatasets({})
                        continue
                    page.setup(*args, **kwargs)
                    for dataset, data in page(*args, **kwargs):
                        yield query, Greatschools_Links_WebDatasets({dataset: data})

    
def main(*args, **kwargs): 
    delayer = Greatschools_Links_WebDelayer("random", wait=(15, 30))
    scheduler = Greatschools_Links_WebScheduler(*args, file=REPORT_FILE, **kwargs)
    downloader = Greatschools_Links_WebDownloader(*args, repository=REPOSITORY_DIR, **kwargs)
    queue = scheduler(*args, **kwargs)
    downloader(*args, delayer=delayer, queue=queue, **kwargs)
    LOGGER.info(str(downloader))
    for results in downloader.results:
        LOGGER.info(str(results))
    if not bool(downloader):
        raise downloader.error
    

if __name__ == "__main__":
    sys.argv += ["state=CA", "city=Bakersfield"]
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    inputparser = InputParser(proxys={"assign": "=", "space": "_"}, parsers={}, default=str)
    inputparser(*sys.argv[1:])
    main(*inputparser.arguments, **inputparser.parameters)

















