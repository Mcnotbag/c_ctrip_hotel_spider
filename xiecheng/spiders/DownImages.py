# -*- coding: utf-8 -*-
import hashlib
import json
import re
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
    name = 'Images'
    allowed_domains = ['ctrip.com']
    start_urls = ['http://hotels.ctrip.com/hotel']
    detail_urls = "http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID={}&hotel={}&EDM=F&roomId=&IncludeRoom=&city=&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate={}&depDate={}&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&equip=&filter=&productcode=&couponList=&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=750b5f087458de3f433a67698ee9f870d8a163be315660fb089a3c31b4680d08&callback=CASpIrzFXZrVOhxTPOo&_=1526703447992"
    nowdate, tomorrwdate = process_urltime()
    # 获取开业时间和电话url
    phone_url = "http://hotels.ctrip.com/hotel/{}.html?isFull=F"
    # 所有图片url
    pic_url = "http://hotels.ctrip.com/Domestic/tool/AjaxLoadPictureAlbum.aspx?hotel={HId}&city={CId}&istaiwan=0"
    page = 1

    conn = pymssql.connect(host='119.145.8.188:16433', user='sa', password='Ecaim6688', database='HotelSpider')
    cur = conn.cursor()
    def start_requests(self):
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
                "page": "1",
            },
            dont_filter=True
        )

    def parse(self, response):
        hotels = self.select_hotels()
        with open("Images_hotel.txt", "r", encoding="utf-8") as f:
            hotels_str = f.read()
        over_hotel = set(hotels_str.split(","))
        for hotel in hotels:
            if hotel in over_hotel:
                continue
            with open("Images_hotel.txt",'a',encoding="utf-8") as f:
                f.write(hotel+",")
            sleep(2)
            item = {}
            item["HId"] = str(hotel)
            headers3 = {
                "Referer": "http://hotels.ctrip.com/hotel/{}.html?isFull=F".format(item["HId"]),
                "Host": "hotels.ctrip.com",
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
            }
            try:
                pic_response = requests.get(self.pic_url.format(HId=item["HId"], CId="1"), headers=headers3, timeout=10)
            except:
                pass
            else:
                pic_html = etree.HTML(pic_response.content.decode())
                item["pic_title"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@alt")
                item["pic_url"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@data-bigpic")
                item["pic_id"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/img/@pid")
                item["pic_Ttitle"] = pic_html.xpath("//div[@id='J_OffiPicDiv']/div[@class='pic_right']/a/@pic-type")
                item["City"] = "广州"
                yield deepcopy(item)


    def select_hotels(self):
        sql = "select HId from HotelSpider.dbo.Hotel where  Source='1' and City='广州'"
        # sql = "select HId from HotelSpider.dbo.Hotel where  Source='1' and City='长沙'"

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        import numpy
        hotels = []
        li = numpy.array(ret)
        for i in li:
            hotel = i[0]
            hotels.append(hotel)
        return hotels

    def __del__(self):
        self.cur.close()
        self.conn.close()


