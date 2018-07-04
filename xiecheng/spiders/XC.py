# -*- coding: utf-8 -*-
import hashlib
import json
import re
from copy import deepcopy
from pprint import pprint
from time import sleep
from urllib import parse
from urllib.parse import to_bytes

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

"""
图片:http://hotels.ctrip.com/Domestic/Tool/AjaxLoadPictureAlbum.aspx?isFromList=T&city=30&istaiwan=0&hotel=6461667
城市:http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx
列表页：http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx
详情页：http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID=6461667&hotel=6461667&EDM=F&roomId=&IncludeRoom=&city=30&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate=2018-05-08&depDate=2018-05-09&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&equip=&filter=&productcode=&couponList=&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=57edcb55890ff7224025d0f76c03131f9a23079ddae2ae5c00fb75b9a0c23fc5&callback=CASqinzsxIZFTSRGrMk&_=1525749361046
       "http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx"
"""
redis_server = Redis(host="119.145.8.188",port=6379,decode_responses=True)
def process_urltime():
    now = datetime.date.today()
    tomorrw = now + datetime.timedelta(days=1)
    return now, tomorrw
# 获取所有城市编号
def allcity():

    city = []
    try:
        city = redis_server.blpop("city")
    except Exception as e:
        print(e)
    if city:
        city = city[1].replace("/",'')
        print("city---%s"%city)
        return city
    else:
        return None

class XcSpider(RedisSpider):
    name = 'XC'
    allowed_domains = ['ctrip.com']
    start_urls = ['http://hotels.ctrip.com/hotel']
    nowdate, tomorrwdate = process_urltime()
    detail_urls = "http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID={}&hotel={}&EDM=F&roomId=&IncludeRoom=&city=&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate={}&depDate={}&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&equip=&filter=&productcode=&couponList=&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=750b5f087458de3f433a67698ee9f870d8a163be315660fb089a3c31b4680d08&callback=CASpIrzFXZrVOhxTPOo&_=1526703447992"
    # 获取开业时间和电话url
    phone_url = "http://hotels.ctrip.com/hotel/{}.html?isFull=F"
    # 所有图片url
    pic_url = "http://hotels.ctrip.com/Domestic/tool/AjaxLoadPictureAlbum.aspx?hotel={HId}&city={CId}&istaiwan=0"
    City = allcity()
    temp_city = City
    page = 1
    def start_requests(self):
        self.log("start log")
        print(self.nowdate)
        print(self.tomorrwdate)
        # 获取当前城市
        cur_city = self.City
        print("*"*100)
        cur_city = eval(cur_city)
        city_py = cur_city[0].replace("/", '')
        city_id = cur_city[1].replace("/", '')
        print("*"*100)
        yield scrapy.FormRequest(
            url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
            formdata={
                "__VIEWSTATEGENERATOR": "DB1FBB6D",
                "StartTime": str(self.nowdate),
                "DepTime": str(self.tomorrwdate),
                "operationtype": "NEWHOTELORDER",
                "cityId": str(city_id),
                "cityPY": str(city_py),
                "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                "page": "1",
            },
            dont_filter=True
        )

    def parse(self, response):
        html_str = response.body.decode()
        paging_str = None
        html_json = None
        try:
            html_json = json.loads(html_str)
        except Exception as e:
            # print(e)
            html_str = re.sub(r'\w+\\[\w‘]+', self.sub_str, html_str)
        try:
            html_json = json.loads(html_str)
        except Exception as e:
            # print(e)

            html_str = parse.unquote(parse.quote(html_str).replace('%20%08',''))
        try:
            html_json = json.loads(html_str)
        except Exception as e:
            print(e)
            with open("list.html","w",encoding="utf-8") as f:
                f.write(html_str)
        try:
            self.page = html_json["HotelMaiDianData"]["value"]["pageindex"]
            hotels = html_json["hotelPositionJSON"]
            hotels_street = html_json["hotelMapStreetJSON"]
            paging_str = html_json["paging"]
        except Exception as e:
            with open("json_error.html","w",encoding="utf-8") as f:
                f.write(html_str)
            print(e)
            print("列表页转换成json出错")
            print("将跳过这一页")
            with open("list.html","w",encoding="utf-8") as f:
                f.write(response.body.decode())
            cur_city = self.City
            cur_city = eval(cur_city)
            city_py = cur_city[0].replace("/", '')
            city_id = cur_city[1].replace("/", '')
            if self.page == '' or self.page == None or self.page == 0:
                self.page = 1
            yield scrapy.FormRequest(
                url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
                formdata={
                    "__VIEWSTATEGENERATOR": "DB1FBB6D",
                    "StartTime": str(self.nowdate),
                    "DepTime": str(self.tomorrwdate),
                    "operationtype": "NEWHOTELORDER",
                    "cityId": str(city_id),
                    "cityPY": str(city_py),
                    "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                    "page": str(int(self.page)+2),
                },
                callback=self.parse,
                errback=self.parse_error
            )
        else:
            # # print(html_json["hotelPositionJSON"])
            # hotels = html_json["hotelPositionJSON"]
            # hotels_street = html_json["hotelMapStreetJSON"]
            # paging_str = html_json["paging"]
            # self.page = html_json["HotelMaiDianData"]["value"]["pageindex"]
            for hotel in hotels:
                item = {}
                item["Source"] = "1"
                item["HId"] = hotel["id"] if hotel["id"] != [] else ''
                item["Name"] = hotel["name"] if hotel["name"] != [] else ''
                item["HUrl"] = "http://hotels.ctrip.com/" + hotel["url"] if hotel["url"] != [] else ''
                item["Address"] = hotel["address"] if hotel["address"] != [] else ''
                item["Score"] = hotel["score"] if hotel["score"] != [] else 0
                item["City"] = html_json["HotelMaiDianData"]["value"]["cityname"] if html_json["HotelMaiDianData"] != [] else ''
                item["Star"] = hotel["star"] if hotel["star"] != [] else ''
                item["Stardesc"] = hotel["stardesc"] if hotel["stardesc"] != [] else ''
                item["Cover"] = hotel["img"] if hotel["img"] != [] else ''
                item["street"] = hotels_street[item["HId"]]["amap"]["pos"] if hotels_street[item["HId"]] != [] else ''
                # # 获取phone和开业时间，酒店介绍, 装修时间，房间数量
                headers2 = {
                    "Referer":"http://hotels.ctrip.com/hotel/shenzhen30",
                    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
                }
                response_phone = requests.post(self.phone_url.format(item["HId"]),headers=headers2)
                response_phone_html = etree.HTML(response_phone.content.decode())
                "0755-00000000 传，18500000000,0755-00000000 / 18500000000"
                item["Phone"] = re.findall(r'<span id="J_realContact" data-real="电话([0-9\-\\/ ]{27}|[0-9\-]{19}|[0-9\-]{18}|[0-9\-]{13}|[0-9\-]{12}|[0-9\-]{11}|[0-9]{10})',response_phone.content.decode())
                item["KYdate"] = re.findall(r'(\d+)年开业',response_phone.content.decode())
                item["ZXdate"] = re.findall(r";([0-9]+)年装修",response_phone.content.decode())
                item["Description"] = re.findall(r'itemprop="description">(.*?)</span>',response_phone.content.decode())
                item["Roomtotal"] = re.findall(r";([0-9]+)间房",response_phone.content.decode())
                item["business"] = re.findall(r'class="detail_add_area" .*?>(.*?)</a>',response_phone.content.decode())
                item["Area"] = response_phone_html.xpath(".//span[@id='ctl00_MainContentPlaceHolder_commonHead_lnkLocation']/text()")
                item["Hlabel"] = response_phone_html.xpath(".//div[@class='hotel_info_comment detail_content']/div[@class='special_label']/i/text()")
                # 获取酒店设施
                item["Hfacility"] = []
                tr1_list = response_phone_html.xpath("//div[@id='J_htl_facilities']//tr")
                for tr1 in tr1_list[:-1]:
                    tr_dict = dict()
                    title1 = tr1.xpath("./th/text()")[0]
                    facility1 = tr1.xpath("./td/ul/li/@title")
                    tr_dict[title1] = facility1
                    item["Hfacility"].append(tr_dict)
                # 获取周边设施
                item["Ofacility"] = []
                tr2_list = response_phone_html.xpath("//h2[@class='detail_title' and text()='周边设施']/following-sibling::div[1]//tr")
                for tr2 in tr2_list:
                    tr_dict = dict()
                    title2 = tr2.xpath('./th/text()')[0]
                    facility2 = tr2.xpath('./td/ul/li/text()')
                    if not facility2:
                        continue
                    tr_dict[title2] = facility2
                    item["Ofacility"].append(tr_dict)
                # 获取所有图片
                headers3 = {
                    "Referer":"http://hotels.ctrip.com/hotel/{}.html?isFull=F".format(item["HId"]),
                    "Host":"hotels.ctrip.com",
                    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
                }
                cur_city = eval(self.City)
                city_id = cur_city[1].replace("/", '')
                pic_response = requests.get(self.pic_url.format(HId=item["HId"],CId=city_id),headers=headers3,timeout=10)
                pic_html = etree.HTML(pic_response.content.decode())
                item["pic_title"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@alt")
                item["pic_url"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@data-bigpic")
                item["pic_id"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@pid")

                yield scrapy.Request(
                    self.detail_urls.format(int(item["HId"]), int(item["HId"]), self.nowdate, self.tomorrwdate),
                    headers={
                        "Referer": "http://hotels.ctrip.com/hotel/{}.html?isFull=F".format(item["HId"])
                    },
                    callback=self.detail_parse,
                    meta={"item": deepcopy(item),"dont_redirect":True},
                    errback=self.parse_error
                )
        # 翻页处理
        if paging_str is not None:
            cur_page,total_page = \
            re.findall(r'<input class="c_page_num" id="txtpage" type="text" value="(\d+)"data-pagecount=(\d+) name="" />',
                       paging_str)[0]
            self.page = cur_page
            print("*"*50)
            print("当前页数----%s"%cur_page)
            print("总页数----%s"%total_page)
            print("*"*50)
            if int(cur_page) == int(total_page):
                self.City = allcity()

            cur_city = self.City
            cur_city = eval(cur_city)
            self.temp_city = cur_city
            city_py = str(cur_city[0]).replace("/", '')
            city_id = str(cur_city[1]).replace("/", '')

            if int(cur_page) < int(total_page):
                print("*"*50)
                print("当前页数%s页"% cur_page)
                print("self_page--%s"%self.page)
                print("当前城市--%s" % cur_city)
                print("self城市--%s"%self.City)
                self.nowdate,self.tomorrwdate = process_urltime()
                yield scrapy.FormRequest(
                    url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
                    formdata={
                        "__VIEWSTATEGENERATOR": "DB1FBB6D",
                        "StartTime": "{}".format(self.nowdate),
                        "DepTime": "{}".format(self.tomorrwdate),
                        "operationtype": "NEWHOTELORDER",
                        "cityId": str(city_id),
                        "cityPY": str(city_py),
                        "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                        "page": str(int(cur_page)+1)
                    },
                    callback=self.parse,
                    errback=self.parse_error
                )
            # 翻完所有页进行城市切换
            else:
                print("*"*100)
                print("切换城市%s" %self.City)
                print("*"*100)
                if cur_city:
                    self.nowdate,self.tomorrwdate = process_urltime()
                    yield scrapy.FormRequest(
                        url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
                        formdata={
                            "__VIEWSTATEGENERATOR": "DB1FBB6D",
                            "StartTime": str(self.nowdate),
                            "DepTime": str(self.tomorrwdate),
                            "operationtype": "NEWHOTELORDER",
                            "cityId": str(city_id),
                            "cityPY": str(city_py),
                            "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                            "page": "1",
                        },
                        callback=self.parse,
                        errback=self.parse_error,
                    )
        else:
            print("*"*50)
            print("发生错误切换城市-当前城市%s" %self.City)
            cur_city = allcity()
            self.City = cur_city
            cur_city = eval(cur_city)
            city_py = cur_city[0].replace("/", '')
            city_id = cur_city[1].replace("/", '')
            print("发生错误切换城市-下一城市%s" % cur_city)
            print("*"*50)
            # 如果发生错误 把当前城市重新放入redis中
            redis_server.rpush("city",self.temp_city)
            print("看cur_city是不是为空")
            print("cityid---%s"%city_id)
            print("citypy----%s"%city_py)
            print(cur_city)
            if cur_city:
                self.nowdate, self.tomorrwdate = process_urltime()
                yield scrapy.FormRequest(
                    url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",
                    formdata={
                        "__VIEWSTATEGENERATOR": "DB1FBB6D",
                        "StartTime": str(self.nowdate),
                        "DepTime": str(self.tomorrwdate),
                        "operationtype": "NEWHOTELORDER",
                        "cityId": str(city_id),
                        "cityPY": str(city_py),
                        "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
                        "page": "1",
                    },
                    callback=self.parse,
                    errback=self.parse_error,
                )

    def detail_parse(self, response):
        item = response.meta["item"]
        try:
            json_response = json.loads(response.body.decode())
        except Exception as e:
            print(e)
            with open("now_detail.html", "w", encoding="utf-8") as f:
                f.write(response.body.decode())
        else:
            detail_response = json_response["html"]
            item["index_price"] = json_response["PriceZl"]
            detail_html = etree.HTML(detail_response)
            tr_list = detail_html.xpath("//tr[@class='clicked hidden']")

            """
            A  32  X  X  X   [0]
            B  43  c  c  c    [1]
            C  45  f
            """

            for tr in tr_list[1:]:
                item["Roomtype"] = {}
                if "Rtitle" not in item["Roomtype"]:
                    item["Roomtype"]["Rtitle"] = tr.xpath(".//div[@class='hrd-title']/text()")
                    item["Roomtype"]["Rtitle"] = [i.replace(" ", "").replace("\n", "") for i in item["Roomtype"]["Rtitle"]][0].replace("\t", "") if item["Roomtype"]["Rtitle"] != [] else ''
                    item["Roomtype"]["RId"] = tr.xpath(".//div[@class='hrd-title']/../@id")
                    item["Roomtype"]["RId"] = item["Roomtype"]["RId"][0].replace("rdn", "") if item["Roomtype"]["RId"] != [] else []
                    item["Roomtype"]["Rimage"] = tr.xpath(".//div[@class='J_slider_box_in']//li/img/@src")
                    item["Roomtype"]["Rarea"] = tr.xpath(".//div[@class='hrd-info-base']/ul/li[1]/text()")
                    item["Roomtype"]["Rarea"] = [i.replace("\n", "").replace(" ", "") for i in item["Roomtype"]["Rarea"]]
                    item["Roomtype"]["Rbed"] = tr.xpath(".//div[@class='hrd-info-base']/ul/li[3]/text()")
                    item["Roomtype"]["Rwindow"] = tr.xpath(".//div[@class='hrd-info-base']/ul/li[4]/text()")
                    item["Roomtype"]["Rfloor"] = tr.xpath(".//div[@class='hrd-info-base']/ul/li[2]/text()")
                    item["RoomFacility"] = tr.xpath(".//ul[@class='hrd-allfac-list']/li/text()")
                    tr2_list = detail_html.xpath("//tr[@brid='{}']".format(item["Roomtype"]["RId"]))
                    #    测试改动
                    try:
                        item["Roomtype"]["Rcover"] = re.findall(r'{"RoomUrl":"(.*?)","RoomID":%s'%int(item["Roomtype"]["RId"]),detail_response)
                        item["Roomtype"]["Rcover"] = item["Roomtype"]["Rcover"][0] if item["Roomtype"]["Rcover"] != [] else ''
                    except Exception as e:
                        print(e)
                        print(item["Roomtype"]["RId"])
                        item["Roomtype"]["Rcover"] = ''


                    for tr2 in tr2_list:
                        item["Roomtype"]["room"] = {}
                        item["Roomtype"]["room"]["breakfast"] = tr2.xpath(".//td[@class='col4']/text()")
                        item["Roomtype"]["room"]["PId"] = tr2.xpath("./@guid")
                        item["Roomtype"]["room"]["price"] = tr2.xpath(".//span[@class='base_price']/text()")
                        item["Roomtype"]["room"]["people"] = tr.xpath(".//td[@class='col_person']/span/@title")
                        # print(item["Hlabel"])

                        yield deepcopy(item)


    def del_fingerprint(self,url,method):
        fp = hashlib.sha1()
        fp.update(to_bytes(method))
        fp.update(to_bytes(canonicalize_url(url)))
        fp.update(b'')
        redis_server.srem('XC:dupefilter',fp.hexdigest())


    def parse_error(self,error):
        print("有没有进入到parse_error------")
        if error.check(HttpError):
            response = error.value.response
            request = error.request
            self.logger.error("请求错误 这是----%s" % response.url)
            self.del_fingerprint(response.url,request.method)
            raise CloseSpider

    def sub_str(self,str):
        l = str.group().split('\\')
        return l[0] + r'\\' + l[1]