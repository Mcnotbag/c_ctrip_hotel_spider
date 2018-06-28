import json

import re
import uuid

from lxml import etree

import requests

# headers = {
#     "Referer":"http://hotels.ctrip.com/hotel/6461667.html?isFull=F",
#     "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
# }
# response = requests.get("http://hotels.ctrip.com/Domestic/tool/AjaxHote1RoomListForDetai1.aspx?psid=&MasterHotelID=6461667&hotel=6461667&EDM=F&roomId=&IncludeRoom=&city=30&showspothotel=T&supplier=&IsDecoupleSpotHotelAndGroup=F&contrast=0&brand=594&startDate=2018-05-10&depDate=2018-05-11&IsFlash=F&RequestTravelMoney=F&hsids=&IsJustConfirm=&contyped=0&priceInfo=-1&abForHuaZhu=&defaultLoad=T&TmFromList=F&RoomGuestCount=1,1,0&eleven=57edcb55890ff7224025d0f76c03131f9a23079ddae2ae5c00fb75b9a0c23fc5&callback=CASqinzsxIZFTSRGrMk&_=1525749361046",headers=headers)
# json_response = json.loads(response.content.decode("utf-8")
# )
# print(json_response["html"])
# with open("detail.html","w",encoding="utf8") as f:
#     f.write(json_response["html"])
# html = etree.HTML(json_response["html"])
# td_list = html.xpath("//tr[@class='clicked hidden']")
# print(len(td_list))
# print(td_list[0].xpath("//div[@class='hrd-info-base']//text()"))
#
#
# a = set()
# a.add("name")
# a.add("nam3")
# print(a)
#
# c = ["2018"][0]
# print(c)
# from redis import Redis
# # # # # #
# redis_server = Redis(host="111.230.34.217",port=6379,decode_responses=True)
# # # # # #
# city_headers = {
#     'Referer': 'http://hotels.ctrip.com/hotel',
#     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
# }
# # city_response = requests.get("http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx", headers=city_headers)
# # # ret = re.findall(r'data:"(.*?)"', city_response.content.decode())
# # # ret = set(ret)
# # ret_hot = re.findall(r'suggestion={热门:\[(.*?)\],AB',city_response.content.decode())[0]
# # ret_hot = re.findall(r'data:"(.*?)",',ret_hot)
# #
# # for i in ret_hot:
# #     i.replace("/",'')
# #     ret2 = i.split("|")
# #     ret2.pop(1)
# #     print(ret2)
#     # redis_server.rpush("city",ret2)
# data = {
#     "city":"32",
#     "markType":"4",
#     "newversion":"T"
# }
# response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",headers=city_headers,data=data)
# HTML_location = etree.HTML(response_location.content.decode())
# scenis = HTML_location.xpath(".//div[@title]/text()")
# print(scenis)
# #
# for i in range(100):
#     city = redis_server.blpop("city2")
#     city_item = city[1]
#     city = eval(city[1])
#
#     city_py = city[0].replace("/",'')
#     city_id = city[1].replace("/",'')
#
#     print(city_py,city_id)



# image_str = "//dimg10.c-ctrip.com/images/200q0k000000bzthzD3D9_R_300_225.jpg', '//dimg12.c-ctrip.com/images/200i070000002sogcB363_R_300_225.jpg', '//dimg10.c-ctrip.com/images/200v070000002nysh2A1A_R_300_225.jpg', '//dimg11.c-ctrip.com/images/200j070000002sogfEE18_R_300_225.jpg', '//dimg11.c-ctrip.com/images/2004070000002nysk1972_R_300_225.jpg', '//dimg11.c-ctrip.com/images/200k070000002sogp9294_R_300_225.jpg"
# print(len(image_str))

# print(str(['0755-29995711', '18038188743']))

# phone_str = str(['0755-29995711', '18038188743'])
# phone_list = eval(phone_str)
# phone_join = ';'.join(phone_list)
# print(phone_join)


# chengdu = "[/'chengdu/','29']"
# chengdu= chengdu.replace("/",'')
# print(chengdu)
#



# city = redis_server.blpop("city")
# print(type(city))
# print(city)


# l = "Coast派赛菲特床上用品、Sealy丝涟床垫以及L'Occitane欧舒丹沐浴用品，确保客人获得尊贵舒适的入住体验。酒店的钻石宴会厅面积达1200平方米，可同时容纳90桌宴席；此外，480平方米的红宝石宴会厅及4个多功能会议厅可满足多种形式的会议宴会。酒店还拥有在上海市区酒店内的婚礼教堂、5000平方米的非SPA中心和以声光影像科技讲述上海美食历史的非SPA–食光隧道、4家餐厅、酒吧，还有适合孩子玩耍的儿童乐园；是你闹中取静，带着家人度假休闲的理想之所。"
# l = l.replace("'",'')
# print(l)


# import hashlib
#
# jiami = hashlib.md5("123".encode("utf-8")).hexdigest()[8:-8]
# print(jiami)