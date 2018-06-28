import json
import re
from urllib import parse

from lxml import etree
from pprint import pprint

import requests


# data ={
#                 "__VIEWSTATEGENERATOR": "DB1FBB6D",
#                 "StartTime": "2018-05-16",
#                 "DepTime": "2018-05-17",
#                 "operationtype": "NEWHOTELORDER",
#                 "cityId": "1",
#                 "cityPY": "beijing",
#                 "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
#                 "page": "15"
#             }
# response = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",data=data)
# json_html = json.loads(response.content.decode())
# with open("10.html","w",encoding="utf-8") as f:
#     f.write(str(json_html))
#
# cur,nex = re.findall(r'<input class="c_page_num" id="txtpage" type="text" value="(\d+)"data-pagecount=(\d+) name="" />',page_str)[0]
#
#
# print(cur)
# print(nex)
# print("当前页数为%s页")

# 电话测试
# headers2 = {
#                     "Referer":"http://hotels.ctrip.com/hotel/shenzhen30",
#                     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
#                 }
# response = requests.post("http://hotels.ctrip.com/hotel/391750.html?isFull=F",headers=headers2)
# response_html = etree.HTML(response.content.decode())
# # 周边设施
# tr_list = response_html.xpath("//h2[@class='detail_title' and text()='周边设施']/following-sibling::div[1]//tr")
# for tr in tr_list:
#     title = tr.xpath('./th/text()')
#     facility = tr.xpath('./td/ul/li/text()')
#     print(title,facility)

# 酒店设施
# tr_list = response_html.xpath("//div[@id='J_htl_facilities']//tr")
# for tr in tr_list[:-1]:
#     title = tr.xpath("./th/text()")
#     facility = tr.xpath("./td/ul/li/@title")
#     print(title,facility)
# phone = re.findall(r'<span id="J_realContact" data-real="电话([0-9\-\\/ ]{27}|[0-9\-]{19}|[0-9\-]{13}|[0-9\-]{12}|[0-9\-]{11}|[0-9]{10})',response.content.decode())
# # print(phone)
# #
# # print(response.content.decode())
# with open("phone.html","w",encoding="utf-8") as f:
#     f.write(response.content.decode())
# # ZX_date = re.findall(r";([0-9]+)年装修",response.content.decode())
# # totalroom = re.findall(r";([0-9]+)间房",response.content.decode())
# # print(totalroom)
# # print(ZX_date)
# area = re.findall(r'class="detail_add_area" .*?>(.*?)</a>',response.content.decode())
# print(str(area))
# area2 = ','.join(area)
# print(area2)


# headers2 = {
#                     "Referer":"http://hotels.ctrip.com/hotel/shenzhen30",
#                     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
#                 }
#
#
# formdata={
#                         "__VIEWSTATEGENERATOR": "DB1FBB6D",
#                         "StartTime": "2018-05-18",
#                         "DepTime": "2018-05-19",
#                         "operationtype": "NEWHOTELORDER",
#                         "cityId": "1",
#                         "cityPY": "Beijing",
#                         "AllHotelIds": "6461667%2C457399%2C2765033%2C533870%2C14154841%2C2247932%2C419559%2C652002%2C12135159%2C6636949%2C450229%2C7588938%2C474461%2C433471%2C6469756%2C654875%2C1199422%2C346485%2C3667172%2C9151493%2C456141%2C5153544%2C11237662%2C13574647%2C529079",
#                         "page": "1",
#                     }
#
# response = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx",data=formdata,headers=headers2)
#
# pprint(response.content.decode())

# ip = requests.get("http://dynamic.goubanjia.com/dynamic/get/30f846d90bf66fccdc0448bd294bb2c3.html?sep=3")
# print(ip.content.decode())


# 333
# def sub_str(str):
#     l = str.group().split('\\')
#     return l[0] + r'\\' + l[1]
# #
# # def sub2_str(str):
# #     l = str.group().split('')
# #
# with open("..\\..\\list.html","r",encoding="utf-8") as f:
#     response = f.read()
#
# html_str = re.sub(r'\w+\\\w+', sub_str, response)
# ret = re.findall(r'\w+\\[\w‘]+',response)
# print(ret)
# try:
#     html_json = json.loads(html_str)
# except Exception as e:
#     print(str(e))
#     print(html_str[65495:65496])
#     print("end")
#     new_str = html_str[65490:65499].replace("\n",'').replace("\t",'').replace('\xa0','')
#     newnew_str = ''.join(new_str)
#     print(newnew_str)
#     print("end2")


# print(len("cQuery.jsonpResponse = "))

# response = requests.get("http://hotels.ctrip.com/domestic/tool/AjaxCityZoneNew.aspx?city=378")
# with open("error.html","r",encoding="utf-8") as f:
#     content = f.read()
#     j = content[630:634]
#     print(j)
#     url_j = parse.quote(j)
#     print(url_j)
# ret = json.loads(content[23:])
# print(ret)

#
# li = [{'网络': '客房WIFI免费,房间内高速上网,公共区WIFI免费'}, {'交通设施': '收费停车场'}, {'交通服务': '接站服务,租车服务'}, {'休闲娱乐': '健身室'}, {'前台服务': '行李寄存,24小时前台,24小时大堂经理,专职行李员,专职门童,礼宾服务,免费旅游交通图,旅游票务,叫醒服务,邮政服务,前台保险柜,信用卡结算,快速办理入住'}, {'餐饮服务': '西餐厅,日式餐厅,咖啡厅,酒吧,送餐服务'}, {'商务服务': '商务服务'}, {'通用设施': '电梯,大堂吧,非经营性休息区,公共音响系统,所有场所严禁吸烟,公用区监控系统'}, {'其他服务': '洗衣服务,外送洗衣,干洗,熨衣服务'}]
#
# for i in li:
#     keys = list(i.keys())[0]
#     print(keys)

