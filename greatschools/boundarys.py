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
import json
import regex as re
from abc import ABC
from seleniumwire.utils import decode
from datetime import date as Date
from collections import OrderedDict as ODict

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
MODULE_DIR = os.path.abspath(os.path.join(MAIN_DIR, os.pardir))
ROOT_DIR = os.path.abspath(os.path.join(MODULE_DIR, os.pardir))
RESOURCE_DIR = os.path.join(ROOT_DIR, "resources")
SAVE_DIR = os.path.join(ROOT_DIR, "save")
REPOSITORY_DIR = os.path.join(SAVE_DIR, "greatschools")
REPORT_FILE = os.path.join(REPOSITORY_DIR, "boundary.csv")
QUEUE_FILE = os.path.join(REPOSITORY_DIR, "boundary.zip")
DRIVER_EXE = os.path.join(RESOURCE_DIR, "chromedriver.exe")
NORDVPN_EXE = os.path.join("C:/", "Program Files", "NordVPN", "NordVPN.exe")
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utilities.inputs import InputParser
from utilities.shapes import Shape, Geometry
from files.dataframes import DataframeFile
from files.shapes import ShapeRecord
from webscraping.webtimers import WebDelayer
from webscraping.webvpn import Nord_WebVPN, WebVPNProcess
from webscraping.webdrivers import WebBrowser
from webscraping.weburl import WebURL
from webscraping.webpages import WebBrowserPage, ContentMixin
from webscraping.webpages import WebConditions, ExecuteError
from webscraping.weberrors import WebPageError
from webscraping.webloaders import WebLoader
from webscraping.webquerys import WebQuery, WebDataset
from webscraping.webqueues import WebScheduler, WebQueueable, WebQueue
from webscraping.webdownloaders import WebDownloader
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


QUERYS = ["GID"]
DATASETS = ["shapes.zip"]


captcha_xpath = r"//*[contains(@class, 'Captcha') or contains(@id, 'Captcha')]"
captcha_webloader = WebLoader(xpath=captcha_xpath, timeout=5)
identity_pattern = "(?<=\/)\d+|(?<=schoolId=)\d+"
identity_parser = lambda x: str(re.findall(identity_pattern, x)[0])
getitem_iterator = lambda contents, key, default: (key, contents.get(key, None)) if isinstance(key, str) else (key, getitem_iterator(contents[key[0]], key[1] if len(key) == 1 else key[1:], default))


class Greatschools_Boundary_HTMLWebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, **kwargs): return ["school-district-boundaries-map"]
    @staticmethod
    def parm(*args, GID, state, **kwargs): return {"schoolId": str(GID), "state": str(state)}


class Greatschools_Boundary_JSONWebURL(WebURL, protocol="https", domain="www.greatschools.org"):
    @staticmethod
    def path(*args, GID, **kwargs): return ["gsr", "api", "schools", str(GID)]
    @staticmethod
    def parm(*args, state, **kwargs): return {"state": str(state), "extras": "boundaries"}


class Greatschools_Captcha(WebCaptcha, loader=captcha_webloader, optional=True): pass
class Greatschools_Boundary_WebDelayer(WebDelayer): pass
class Greatschools_Boundary_WebBrowser(WebBrowser, files={"chrome": DRIVER_EXE}, options={"headless": False, "images": True, "incognito": False}): pass
class Greatschools_Boundary_WebQueue(WebQueue): pass
class Greatschools_Boundary_WebQuery(WebQuery, WebQueueable, fields=QUERYS): pass
class Greatschools_Boundary_WebDataset(WebDataset, ABC, fields=DATASETS): pass


class Greatschools_Boundary_WebScheduler(WebScheduler, fields=QUERYS):
    @staticmethod
    def GID(*args, state, city=None, citys=[], zipcode=None, zipcodes=[], **kwargs):
        if not os.path.exists(QUEUE_FILE):
            return []
        assert all([isinstance(item, (str, type(None))) for item in (zipcode, city)])
        assert all([isinstance(item, list) for item in (zipcodes, citys)])
        zipcodes = list(set([item for item in [zipcode, *zipcodes] if item]))
        citys = list(set([item for item in [city, *citys] if item]))
        parsers = {"address": Address.fromstr}
        with DataframeFile(file=QUEUE_FILE, mode="r", parsers=parsers, parser=str) as reader:
            dataframe = reader(header=["zipcode", "type", "city", "state", "county"])
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


page_mixins = (ContentMixin,)
page_contents = (Greatschools_WebConditions,)


class Greatschools_Boundary_WebPage(WebBrowserPage, mixins=page_mixins, contents=page_contents):
    @staticmethod
    def date(): return {"date": Date.today().strftime("%m/%d/%Y")}
    def query(self): return {"GID": str(identity_parser(self.url))}
    def setup(self, *args, **kwargs): pass

    def execute(self, *args, state, **kwargs):
        query = self.query()
        url = Greatschools_Boundary_JSONWebURL(state=state, **query)
        responses = {request.url: request.response for request in self.driver.requests}
        try:
            response = responses[str(url)]
        except KeyError:
            LOGGER.error("Response URL: {}".format(str(url)))
            for index, key in enumerate(responses.keys()):
                LOGGER.error("Response URL[{}]: {}".format(index, key))
            raise ExecuteError(self)
        contents = json.loads(decode(response.body, response.headers.get('Content-Encoding', 'utf-8')))
        mapping = {"id": "GID", "districtId": "DID", "districtName": "district", "lat": "latitude", "lon": "longitude", "name": "name", "gradeLevels": "grades", "schooltype": "type"}
        record = {key: contents.get(key, None) for key, content in mapping.items()}
        record["address"] = Address(ODict([("street", contents["address"]["street1"]), ("city", contents["address"]["city"]), ("state", contents["state"]), ("zipcode", contents["address"]["zip"])]))
        try:
            values = [tuple(value) for value in list(contents["boundaries"].values())[0]["coordinates"][0][0]]
            shape = Shape[Geometry.RING](values)
            return query, "shapes", ShapeRecord(shape, record)
        except IndexError:
            return query, "shapes", None


class Greatschools_Boundary_WebDownloader(WebVPNProcess, WebDownloader):
    def execute(self, *args, browser, scheduler, delayer, referer="https://www.google.com", **kwargs):
        with scheduler(*args, **kwargs) as queue:
            if not queue:
                return
            with browser() as driver:
                page = Greatschools_Boundary_WebPage(driver, name="GreatSchoolsPage", delayer=delayer)
                with queue:
                    for query in queue:
                        if bool(self.vpn.terminated):
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
                        url = Greatschools_Boundary_HTMLWebURL.fromstr(str(url))
                        try:
                            page.load(str(url), referer=referer)
                            page.setup(*args, **kwargs)
                            fields, dataset, data = page(*args, **kwargs)
                            if bool(data):
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
        with DataframeFile(file=QUEUE_FILE, mode="r", index=False, header=True, parsers={}, parser=str) as reader:
            record = reader()
            urls = record(index="GID", header="link").squeeze()
        url = urls.get(GID)
        return url


def main(*args, **kwargs):
    delayer = Greatschools_Boundary_WebDelayer(name="GreatSchoolsDelayer", method="random", wait=(30, 60))
    browser = Greatschools_Boundary_WebBrowser(name="GreatSchoolsBrowser", browser="chrome", timeout=60)
    scheduler = Greatschools_Boundary_WebScheduler(name="GreatSchoolsScheduler", randomize=True, size=5, file=REPORT_FILE)
    downloader = Greatschools_Boundary_WebDownloader(name="GreatSchools", repository=REPOSITORY_DIR, timeout=60*2)
    vpn = Nord_WebVPN(name="NordVPN", file=NORDVPN_EXE, server="United States", timeout=60*2)
    vpn += downloader
    downloader(*args, browser=browser, scheduler=scheduler, delayer=delayer, **kwargs)
    vpn.start()
    downloader.start()
    downloader.join()
    vpn.stop()
    vpn.join()
    for query, results in downloader.results.items():
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


