# -*- coding: utf-8 -*-
import hashlib
import json
import random
import re
import string
from copy import deepcopy
from pprint import pprint
from time import sleep
from urllib import parse
from urllib.parse import to_bytes

import pymssql
import requests
import scrapy
from redis import Redis
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils import request

from scrapy_redis.spiders import RedisSpider
import datetime
from lxml import etree
from scrapy.utils import request
from scrapy import log
import logging

from w3lib.url import canonicalize_url

from scrapy_redis.scheduler import Scheduler
import execjs
import time

"""
图片:http://hotels.ctrip.com/Domestic/Tool/AjaxLoadPictureAlbum.aspx?isFromList=T&city=30&istaiwan=0&hotel=6461667
城市:http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx
列表页：http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx
详情页：http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID=6461667&hotel=6461667&EDM=F&roomId=&IncludeRoom=&city=30&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate=2018-05-08&depDate=2018-05-09&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&equip=&filter=&productcode=&couponList=&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=57edcb55890ff7224025d0f76c03131f9a23079ddae2ae5c00fb75b9a0c23fc5&callback=CASqinzsxIZFTSRGrMk&_=1525749361046
       "http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx"
"""
redis_server = Redis(host="111.230.34.217",port=6379,decode_responses=True)
def process_urltime():
    now = datetime.date.today()
    tomorrw = now + datetime.timedelta(days=1)
    return now, tomorrw

class XcSpider(RedisSpider):
    name = 'Comment'
    allowed_domains = ['ctrip.com']
    start_urls = ['http://hotels.ctrip.com/hotel']
    nowdate, tomorrwdate = process_urltime()
    detail_urls = "http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID={}&hotel={}&EDM=F&roomId=&IncludeRoom=&city=&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate={}&depDate={}&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&equip=&filter=&productcode=&couponList=&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=750b5f087458de3f433a67698ee9f870d8a163be315660fb089a3c31b4680d08&callback=CASpIrzFXZrVOhxTPOo&_=1526703447992"
    # 获取开业时间和电话url
    phone_url = "http://hotels.ctrip.com/hotel/{}.html?isFull=F"
    # 所有图片url
    pic_url = "http://hotels.ctrip.com/Domestic/tool/AjaxLoadPictureAlbum.aspx?hotel={HId}&city={CId}&istaiwan=0"
    page = 1

    # sql
    conn = pymssql.connect(host='119.145.8.188:16433', user='sa', password='Ecaim6688', database='HotelSpider')
    cur = conn.cursor()

    # 关于评论
    get_token_url = "http://m.ctrip.com/restapi/soa2/10934/hotel/customer/cas?_fxpcqlniredt="  # guuid
    get_token_headers = {
        "Host": "m.ctrip.com",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:56.0) Gecko/20100101 Firefox/56.0",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "http://m.ctrip.com/webapp/hotel/hoteldetail/dianping/5860925.html",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
        "Cookie": "ASP.NET_SessionId=dno4iufil4hepsgolbseekag; _bfa=1.1509700598966.7p7ubv.1.1509700598966.1509700650052.1.11.228032; page_time=1509700600627%2C1509700603250; GUID=09031014210846161441; _abtest_userid=d8e5d589-0367-4e6c-8272-9fe263d31d8a; _RF1={IP}; _RSG=QsqRt5ylnI3TVvmvwKPILB; _RDG=285bf10a1483aa27bf252e85f9a02459fa; _RGUID=34b1bfc1-776f-4d9a-b552-654eb9c397dd; _jzqco=%7C%7C%7C%7C%7C1.1819574141.1509700604622.1509700604622.1509700604623.1509700604622.1509700604623.0.0.0.1.1; _ga=GA1.2.964392904.1509700605; _gid=GA1.2.791572248.1509700605; _gat=1",
        "Connection": "close",
        "x-ctrip-pageid": "228032"
    }

    get_token_body = {
        "callback": "TeEObITYmiShXmHjN",
        "alliance": {
            "ishybrid": 0
        },
        "head": {
            "cid": "09031014210846161441",
            "ctok": "",
            "cver": "1.0",
            "lang": "01",
            "sid": "8888",
            "syscode": "09",
            "auth": None,
            "extension": [
                {"name": "pageid", "value": "228032"},
                {"name": "webp", "value": 0},
                {"name": "referrer", "value": "http://m.ctrip.com/html5/"},
                {"name": "protocal", "value": "http"}]},
        "contentType": "json"
    }

    get_content_url = "http://m.ctrip.com/restapi/soa2/10935/hotel/booking/commentgroupsearch?_fxpcqlniredt="  # guuid
    get_content_headers = get_token_headers
    get_content_param = {"flag": 1,
                         "id": "5860925",
                         "htype": 1,
                         "sort": {"idx": 2, "size": 10, "sort": 2, "ord": 1},
                         "search": {"kword": "", "gtype": 4, "opr": 3, "ctrl": 14, "filters": ["有图"]},
                         "alliance": {"ishybrid": 0},
                         "Key": "c2cb2ba68aa4138dab7799ebd05a2b14487b5df876cfa0e4f053e928d07c3210",
                         "head": {"cid": "09031014210846161441", "ctok": "", "cver": "1.0", "lang": "01", "sid": "8888",
                                  "syscode": "09", "auth": None, "extension":
                                      [{"name": "pageid", "value": "228032"},
                                       {"name": "webp", "value": 1},  # 有问题1改0
                                       {"name": "referrer", "value": "http://m.ctrip.com/html5/"},
                                       {"name": "protocal", "value": "http"}]}, "contentType": "json"}

    hotel_detail_headers = {
        "Host": "m.ctrip.com",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Cookie": 'supportwebp=true; list_hotel_price=%7B%22traceid%22%3A%22100004883-2334de86-ce79-4f6f-9fc4-9c7ff1213476%22%2C%22pageid%22%3A%22212093%22%2C%22searchcandidate%22%3A%7B%22bedtype%22%3A%22%22%2C%22person%22%3A0%7D%2C%22timestamp%22%3A1532589405345%2C%22minpriceroom%22%3A%7B%22avgprice%22%3A598%2C%22currency%22%3A%22RMB%22%2C%22iscanreserve%22%3A1%2C%22isshadow%22%3A0%2C%22isusedcashback%22%3A1%2C%22isusedcoupon%22%3A-1%2C%22roomid%22%3A192083182%7D%7D; JSESSIONID=2ED47FFB3F2CA4DE2A42029B6869DDFE; _abtest_userid=11809c7c-7aca-4732-898d-ceb87207afe2; _RSG=TEKspDQxhG9SDvbkIHyi38; _RDG=28fb6b5876871c29b8109011a3b7705d7b; _RGUID=942beda3-73d9-439d-80ec-f114ae97935c; traceExt=campaign=CHNbaidu81&adid=index; _ga=GA1.2.823875851.1525765916; GUID=09031132412056160558; Mkt_UnionRecord=%5B%7B%22aid%22%3A%224897%22%2C%22timestamp%22%3A1530582434732%7D%5D; ASP.NET_SessionId=yjr2corqe0zdmoguv02xic42; _gid=GA1.2.1520385230.1532589377; _RF1=119.137.61.138; adscityen=Shenzhen; HotelCityID=30split%E6%B7%B1%E5%9C%B3splitShenzhensplit2018-7-26split2018-07-27split0; __zpspc=9.105.1532601790.1532601802.3%231%7Cbaidu%7Ccpc%7Cbaidu81%7C%25E6%2590%25BA%25E7%25A8%258B%7C%23; _bfi=p1%3D102003%26p2%3D102002%26v1%3D612%26v2%3D611; appFloatCnt=131; _bfa=1.1525765913130.2c5ttr.1.1532595719030.1532659210936.113.613.212094; MKT_Pagesource=H5; _jzqco=%7C%7C%7C%7C1532589377481%7C1.337062860.1525765916092.1532601802705.1532659211092.1532601802705.1532659211092.undefined.0.0.418.418',
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
        # "Referer":"https://m.ctrip.com/webapp/hotel/hoteldetail/436187.html?atime=20180726&daylater=0&days=1&contrl=0&pay=0&discount=&latlon=&listindex=1&userLocationSearch=false&anchorid=&hotelid=436187"
    }

    DEFAULT_PAGE_SIZE = 10
    global_pageid = None
    global_prev_ts = None
    global_guuid = None
    global_token = None
    global_prev_fetch_id = None
    variables = [None]

    def start_requests(self):
        print(self.nowdate)
        print(self.tomorrwdate)
        print("*"*100)
        yield scrapy.FormRequest(
            url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
            formdata={
                "__VIEWSTATEGENERATOR": "DB1FBB6D",
                "StartTime": str(self.nowdate),
                "DepTime": str(self.tomorrwdate),
                "operationtype": "NEWHOTELORDER",
                "cityId": str(30),
                "cityPY": str("Shenzhen"),
                "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                "page": "219",
            },
            dont_filter=True
        )

    def parse(self, response):
        hotels = self.select_hotels()
        with open("no_comment.txt", "r", encoding="utf-8") as f:
            hotels_str = f.read()
        no_hotels = set(hotels_str.split(","))

        for hotel in hotels:
            if hotel in no_hotels:
                continue
            sleep(random.randint(5,6))
            page = 2
            try:
                response = self.get_start_req(hotel,"1")
            except:
                continue
            json_response = json.loads(response.content.decode())
            if json_response["rc"] == 200:
                # 记录哪些酒店爬过
                with open("no_comment.txt", "a", encoding="utf-8") as f:
                    f.write(hotel + ",")
                try:
                    total = json_response["groups"][0]["pages"]
                    comments = json_response["groups"][0]["comments"]
                    print(total)
                    print(len(comments))
                except:
                    print("此酒店无评论---%s"%hotel)
                else:
                    for comment in comments:
                        item = {}
                        try:
                            item["HId"] = comment["hid"]
                            item["OId"] = comment["oid"]
                            item["Sall"] = comment["rats"]["all"]
                            item["Shealth"] = comment["rats"]["room"]
                            item["Saround"] = comment["rats"]["arnd"]
                            item["Sserver"] = comment["rats"]["serv"]
                            item["Sfaci"] = comment["rats"]["facl"]
                            item["content"] = comment["text"]
                            item["User"] = comment["nick"]
                            item["Room"] = comment["rname"]
                            item["date"] = comment["date"]
                            item["B_image"] = []
                            item["S_image"] = []
                            item["ImageId"] = []
                            images = comment["imgs"]
                            for image in images:
                                item["ImageId"].append(image["id"])
                                for _ in image["items"]:
                                    if _["type"] == 1:
                                        item["S_image"].append(_["url"])
                                    elif _["type"] == 2:
                                        item["B_image"].append(_["url"])
                            yield deepcopy(item)
                        except:
                            pass
                    while True:
                        sleep(3)
                        if page>total:
                            break
                        try:
                            response = self.get_start_req(hotel,str(page))
                        except :
                            break
                        else:
                            json_response = json.loads(response.content.decode())
                            if json_response["rc"] == 200:
                                # page = json_response["groups"][0]["idx"]
                                try:
                                    total = json_response["groups"][0]["pages"]
                                    comments = json_response["groups"][0]["comments"]
                                except:
                                    pass
                                else:
                                    for comment in comments:
                                        item = {}
                                        try:
                                            item["HId"] = comment["hid"]
                                            item["OId"] = comment["oid"]
                                            item["Sall"] = comment["rats"]["all"]
                                            item["Shealth"] = comment["rats"]["room"]
                                            item["Saround"] = comment["rats"]["arnd"]
                                            item["Sserver"] = comment["rats"]["serv"]
                                            item["Sfaci"] = comment["rats"]["facl"]
                                            item["content"] = comment["text"]
                                            item["User"] = comment["nick"]
                                            item["Room"] = comment["rname"]
                                            item["date"] = comment["date"]
                                            item["B_image"] = []
                                            item["S_image"] = []
                                            images = comment["imgs"]
                                            item["ImageId"] = []
                                            for image in images:
                                                item["ImageId"].append(image["id"])
                                                for _ in image["items"]:
                                                    if _["type"] == 1:
                                                        item["S_image"].append(_["url"])
                                                    elif _["type"] == 2:
                                                        item["B_image"].append(_["url"])
                                            yield deepcopy(item)
                                        except:
                                            pass
                        page += 1

    def get_start_req(self,HId, pageToken):
        global global_guuid, global_token, global_pageid
        if not HId or not pageToken.isdigit():
            raise Exception
        if not self.global_guuid:
            self.fetch_and_refresh_guuid()

        self.get_content_param["id"] = HId
        self.get_content_param["sort"]["idx"] = int(pageToken)
        self.get_content_param["sort"]["size"] = int(10)
        # get_content_param["sort"][""] = int(self.adic["sort"])
        self.get_token(self.get_content_headers["Referer"])
        self.update_param()
        url = self.get_content_url + global_guuid
        req = requests.post(url, data=json.dumps(self.get_content_param), headers=self.get_content_headers)
        return req

    def update_param(self):
        self.get_content_param["Key"] = global_token
        self.get_content_param["head"]["cid"] = global_guuid
        self.get_content_param["head"]["extension"][0]["value"] = global_pageid

    def get_token(self,href, force=False):
        global global_pageid, global_prev_ts, global_guuid, global_prev_fetch_id, global_token
        ts = time.time()
        if not force or not global_prev_ts or ts - global_prev_ts > 60:
            url = self.get_token_url + global_guuid
            self.get_token_body["callback"] = "".join(random.choice(string.ascii_lowercase + string.ascii_uppercase)
                                                 for _ in range(17))
            self.get_token_body["head"]["cid"] = global_guuid

            global_pageid = self.get_token_body["head"]["extension"][0]["value"] = str(random.randint(100000, 999999))
            response = requests.post(url, data=json.dumps(self.get_token_body),
                                     headers=self.get_token_headers)
            global_token = self.decode_token(response.content.decode("utf8"), href)
            global_prev_ts = ts
        return global_token

    def fetch_and_refresh_guuid(self):
        global global_guuid
        url = "http://m.ctrip.com/webapp/hotel/hoteldetail/457399.html?days=1&atime=" + datetime.datetime.now().strftime(
            "%Y%m%d")
        if "Cookie" in self.hotel_detail_headers:
            del self.hotel_detail_headers["Cookie"]
        response = requests.get(url, headers=self.hotel_detail_headers)
        for header, v in response.headers.items():
            if header == "Set-Cookie":
                if "GUID=" in v:
                    global_guuid = v.split(";")[0].split("=")[1]
                if "JSESSIONID" in v:
                    jsession = v.split(";")[0].split("=")[1]

        for header in self.get_token_headers, self.get_content_headers:  # room_headers:
            header["Cookie"] = re.sub("[\s|^]GUID=(\d+)", " GUID=" + global_guuid, header["Cookie"])

    def parse_response(self,response):
        json_obj = json.loads(response.body)
        if not json_obj["groups"]:
            raise self.StatusError.Succeed.EmptyResult
        comments = json_obj["groups"][0]["comments"]
        self.item_list = list()
        for comment in comments:
            item = self.ApiObject.new_comment_obj()
            item["id"] = str(comment["comid"])
            item["subobjects"] = [{
                "id": str(comment["oid"]),
                "name": comment["rname"]
            }]
            item["checkinDate"] = comment["cdate"]
            item["content"] = comment["text"]
            item["commenterIdGrade"] = comment["userinfo"]["star"]
            item["source"] = str(comment["source"])
            if comment["rplist"]:
                item["replies"] = list()
                for reply in comment["rplist"]:
                    item["replies"].append(
                        {
                            "name": reply["name"],
                            "date": reply["date"],
                            "content": reply["text"],
                            "id": str(reply["cid"])
                        }
                    )
            else:
                item["replies"] = None
            item["referId"] = str(comment["hid"])
            if comment["imgs"]:
                item["imageUrls"] = set()
                for all_imgs in comment["imgs"]:
                    for each in all_imgs["items"]:
                        if "http" in each["url"]:
                            item["imageUrls"].add(each["url"])
                        else:
                            item["imageUrls"].add(
                                "http://images4.c-ctrip.com/target/%s_W_640_640_Q50.jpg" % (each["url"],))
                item["imageUrls"] = list(item["imageUrls"])
            else:
                item["imageUrls"] = None
            item["publishDate"] = self.FunctionUtil.datetime2timestamp(comment["date"])
            item["rating"] = comment["rats"]["all"]
            item["commenterScreenName"] = comment["userinfo"]["nick"]
            item["url"] = "http://m.ctrip.com/webapp/hotel/hoteldetail/dianping/" + self.adic["id"] + ".html"
            item["qualRating"] = comment["rats"]["room"]
            item["enviRating"] = comment["rats"]["arnd"]
            item["servRating"] = comment["rats"]["serv"]
            item["infraRating"] = comment["rats"]["facl"]
            self.item_list.append(item)
        if json_obj["groups"][0]["pages"] >= int(self.adic["pageToken"]):
            self.has_next, self.page_token = True, str(int(self.adic["pageToken"]) + 1)
        else:
            self.has_next, self.page_token = False, None

    def decode_token(self,html, href):
        def repl(result):
            self.variables[0] = result.group(1)
            return ""

        fake_js_obj = """
        function createElement(string) { return {body: "html"}; }
        this["require"] = undefined;
        document = {
                body: {"innerHTML" : "<html></html>"},
                referrer: undefined,
                documentElement: { "attributes": { "webdriver": undefined } },
                createElement: createElement
            };
            window = {
                Script: undefined,
                location: {
                    href: undefined
                },
                document: {"$cdc_asdjflasutopfhvcZLmcfl_": undefined },
                indexedDB: true,
                createElement: createElement
            };
            navigator = {
                userAgent: "%s",
                geolocation: true
            };
            location = {
                href: "%s"
            };
            window.location.href = location.href;
        """ % (self.get_content_headers["User-Agent"], href)
        rgx_function = re.compile("res\}\((\[.+?\]).+?String\.fromCharCode\(.+?(\d+)\)", re.DOTALL)
        r = re.search(rgx_function, html)
        arr = json.loads(r.group(1))
        distance = int(r.group(2))
        js_text = u""
        for i in arr:
            js_text += chr(i - distance)
        js_text = js_text.replace(";!function()", "function gensign()")
        js_text = fake_js_obj + re.sub("\s\S+new Function\(.+?\+\s(.+?)\s\+.+?\);", repl, js_text)
        js_text = js_text[:-4] + "return %s; }" % (self.variables[0],)
        js_text = js_text.replace("new Image()", 'a=1')
        sign_u = execjs.compile(js_text)
        sign = sign_u.call("gensign")
        if sign_u == u"老板给小三买了包， 却没有给你钱买房" or not sign.islower() or not sign.isalnum() or len(sign) != 64:
            with open("ctrip_html", "w") as f:
                f.write(html)
            raise Exception
        return sign

    def select_hotels(self):
        sql = "select HId from HotelSpider.dbo.Hotel where HId not in (select HId from HotelSpider.dbo.Comment) and Source='1' and City='郑州'"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        import numpy
        li = numpy.array(ret)
        hotels = list()
        for i in li:
            hotel = i[0]
            hotels.append(hotel)
        return hotels
    def __del__(self):
        self.cur.close()
        self.conn.close()