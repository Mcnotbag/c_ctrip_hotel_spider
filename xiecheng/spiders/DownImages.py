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
redis_server = Redis(host="111.230.34.217",port=6379,decode_responses=True)
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
    name = 'Images'
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
            for hotel in hotels:
                item = {}
                item["HId"] = hotel["id"] if hotel["id"] != [] else ''
                headers3 = {
                    "Referer": "http://hotels.ctrip.com/hotel/{}.html?isFull=F".format(item["HId"]),
                    "Host": "hotels.ctrip.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
                }
                cur_city = eval(self.City)
                city_id = cur_city[1].replace("/", '')
                city_name = cur_city[0].replace("/", '')
                pic_response = requests.get(self.pic_url.format(HId=item["HId"], CId=city_id), headers=headers3, timeout=10)
                pic_html = etree.HTML(pic_response.content.decode())
                item["pic_title"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@alt")
                item["pic_url"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@data-bigpic")
                item["pic_id"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@pid")
                item["pic_Ttitle"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/@pic-type")
                item["City"] = html_json["HotelMaiDianData"]["value"]["cityname"] if html_json["HotelMaiDianData"] != [] else ''
                yield deepcopy(item)

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