import hashlib
import json

import re

import pymssql
from pprint import pprint

from lxml import etree
from redis import Redis
import requests
redis_server = Redis(host="111.230.34.217",port=6379,decode_responses=True)

# # # # #
city_headers = {
    'Referer': 'http://hotels.ctrip.com/hotel',
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
}
# city_response = requests.get("http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx", headers=city_headers)
# # ret = re.findall(r'data:"(.*?)"', city_response.content.decode())
# # ret = set(ret)
# ret_hot = re.findall(r'suggestion={热门:\[(.*?)\],AB',city_response.content.decode())[0]
# ret_hot = re.findall(r'data:"(.*?)",',ret_hot)
#
# for i in ret_hot:
#     i.replace("/",'')
#     ret2 = i.split("|")
#     ret2.pop(1)
#     print(ret2)
    # redis_server.rpush("city",ret2)
class Location_XC:
    def __init__(self):
        self.conn = pymssql.connect(host='119.145.8.188:16433', user='sa', password='Ecaim6688', database='HotelSpider')
        self.cur = self.conn.cursor()
    def __del__(self):
        self.cur.close()
        self.conn.close()
    def loc_scenis(self,cityid):
        data = {
            "city":cityid,
            "markType":"4",
            "newversion":"T"
        }
        response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",headers=city_headers,data=data)
        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                location_list = HTML_location.xpath(".//div[@title]/text()")
                print(location_list)
                if location_list != []:
                    self.insert_scenis(cityid,location_list)
    def insert_scenis(self,cityid,scenis_li):
        sql = "insert into Scenic (SId,CId,scenic) values "
        for scenis in scenis_li:
            # sql = "insert into Location (Source,CityId,LocationType,Name) values ('1','%s','景点','%s')" %(str(cityid),str(scenis))
            str_date = (str(cityid) + str(scenis)).encode("utf-8")
            SId = hashlib.md5(str_date).hexdigest()
            sql = sql + "('%s','%s','%s')" % (str(SId),str(cityid),str(scenis))
            sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
            print("插入失败scenis")
        self.conn.commit()

    def loc_business(self,cityid):

        url = "http://hotels.ctrip.com/domestic/tool/AjaxCityZoneNew.aspx?city={}".format(cityid)
        response_location = requests.get(url,headers=city_headers)
        if response_location.status_code == 200:
            if not response_location.content.decode():
                return
            if cityid == "378":
                return
            json_rep_dict = json.loads(response_location.content.decode()[23:])
            for json_rep_key in json_rep_dict:
                rep_list = json_rep_dict[json_rep_key]
                if rep_list:
                    self.insert_business(cityid, rep_list)

    def insert_business(self,cityid,scenis_li):
        sql = "insert into Business (BId,CId,businessName,lat,lng) values "
        for scenis in scenis_li:
            Bname = scenis["name"]
            lat = scenis["lat"]
            lng = scenis["lng"]
            str_date = (str(cityid)+str(Bname)).encode("utf-8")
            BId = hashlib.md5(str_date).hexdigest()
            sql = sql + "('%s','%s','%s','%f','%f')" % (str(BId),str(cityid),str(Bname),float(lat),float(lng))
            sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
            print("插入失败busioness")
        self.conn.commit()

    def loc_subway(self,cityid):
        data = {
            "city": cityid,
            "markType": "2",
            "newversion": "T"
        }
        response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",
                                          headers=city_headers, data=data)
        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                # location_list = HTML_location.xpath(".//div[@title]/text()")
                # print(location_list)
                # if location_list != []:
                    # self.insert_subway(cityid, location_list)
                line_list = HTML_location.xpath("//ul[@class='selectSecList clearfix']/li/text()")
                for line in line_list:
                    sub_station_li = HTML_location.xpath(".//div[@title='%s']/following-sibling::div/@title" % line)
                    # print(line)
                    sub_station_va = HTML_location.xpath(".//div[@title='%s']/following-sibling::div/@data-value" % line)
                    # print(sub_station_li)
                    # print(sub_station_va)
                    for i in sub_station_li:
                        ind = sub_station_li.index(i)
                        self.update_subway(cityid,i,sub_station_va[ind],line)

                    # self.insert_subway(cityid,line,sub_station_li)

    def update_subway(self,cityid,title,value,line):
        str_date = (str(cityid) + str(line) + str(title)).encode("utf-8")
        SId = hashlib.md5(str_date).hexdigest()

        sql = "update Subway set Orders='%d' where SId='%s'" % (int(value),SId)
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
        self.conn.commit()
    def insert_subway(self,cityid,line,scenis_li):
        sql = "insert into Subway (SId,CId,SubwayLine,SubwayStation) values "
        for scenis in scenis_li:
            # sql = "insert into Location (Source,CityId,LocationType,Name) values ('1','%s','景点','%s')" %(str(cityid),str(scenis))
            str_date = (str(cityid)+str(line)+str(scenis)).encode("utf-8")
            SId = hashlib.md5(str_date).hexdigest()
            sql = sql + "('%s','%s','%s','%s')" % (str(SId),str(cityid),str(line),str(scenis))
            sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
            print("插入失败subway")
        self.conn.commit()

    def loc_station(self,cityid):
        data = {
            "city": cityid,
            "markType": "1",
            "newversion": "T"
        }
        response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",
                                          headers=city_headers, data=data)
        # print(response_location.content.decode())
        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                location_list = HTML_location.xpath(".//div[@id]/text()")
                print(location_list)
                if location_list != []:
                    self.insert_station(cityid, location_list)

    def insert_station(self,cityid,scenis_li):
        sql = "insert into Station (StaId,CId,Station) values "
        for scenis in scenis_li:
            # sql = "insert into Location (Source,CityId,LocationType,Name) values ('1','%s','景点','%s')" %(str(cityid),str(scenis))
            str_date = (str(cityid)+str(scenis)).encode("utf-8")
            StaId = hashlib.md5(str_date).hexdigest()
            sql = sql + "('%s','%s','%s')" % (str(StaId),str(cityid),str(scenis))
            sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
            print("插入失败station")
        self.conn.commit()

    def loc_admin(self,cityid):
        data = {
            "city": cityid,
            "markType": "3",
            "newversion": "T"
        }
        response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",
                                          headers=city_headers, data=data)
        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                location_list = HTML_location.xpath(".//div[@title]/text()")
                print(location_list)
                if location_list != []:
                    self.insert_admin(cityid, location_list)

    def insert_admin(self,cityid,scenis_li):
        sql = "insert into District (AId,CId,admin) values "
        for scenis in scenis_li:
            # sql = "insert into Location (Source,CityId,LocationType,Name) values ('1','%s','景点','%s')" %(str(cityid),str(scenis))
            str_date = (str(cityid) + str(scenis)).encode("utf-8")
            AId = hashlib.md5(str_date).hexdigest()
            sql = sql + "('%s','%s','%s')" % (str(AId),str(cityid),str(scenis))
            sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            print(e)
            print("插入失败station")
        self.conn.commit()

    def run(self):

        city_response = requests.get("http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx", headers=city_headers)
        ret = re.findall(r'data:"(.*?)"', city_response.content.decode())
        ret = set(ret)
        ret_hot = re.findall(r'suggestion={热门:\[(.*?)\],AB',city_response.content.decode())[0]
        ret_hot = re.findall(r'data:"(.*?)",',ret_hot)
        for i in ret:
            i.replace("/",'')
            ret2 = i.split("|")
            ret2.pop(1)
            print(ret2)
            # self.loc_business(ret2[1])
            # self.loc_station(ret2[1])
            # self.loc_subway(ret2[1])
            # self.loc_admin(ret2[1])
            # self.loc_scenis(ret2[1])


if __name__ == '__main__':
    location = Location_XC()
    location.run()