import json
import re
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

# headers2 = {
#                     "Referer":"http://hotels.ctrip.com/hotel/shenzhen30",
#                     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
#                 }
# response = requests.post("http://hotels.ctrip.com/hotel/939388.html?isFull=F",headers=headers2)
#
# # print(response.content.decode())
# # with open("phone.html","w",encoding="utf-8") as f:
# #     f.write(response.content.decode())
# ZX_date = re.findall(r";([0-9]+)年装修",response.content.decode())
# totalroom = re.findall(r";([0-9]+)间房",response.content.decode())
# print(totalroom)
# print(ZX_date)

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
