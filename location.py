import hashlib
import json
import queue

import re

import pymssql
import threading
from pprint import pprint
from time import sleep

from lxml import etree
from redis import Redis
import requests
redis_server = Redis(host="119.145.8.188",port=6379,decode_responses=True)

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
data = {
    "__VIEWSTATEGENERATOR":"DB1FBB6D",
    "StartTime":"2018-07-03",
    "DepTime":"2018-07-04",
    "operationtype":"NEWHOTELORDER",
    "cityId":"1",
    "OrderBy":"99",
    "checkIn":"2018-07-03",
    "checkOut":"2018-07-04",
    "hidTestLat":"0%7C0",
    "isfromlist":"T",
    "ubt_price_key":"htl_search_result_promotion",
    "htlFrom":"hotellist",
    "markType":"0",
    "a":"0",
    "contrast":"0",
    "attachDistance":"0",
    "contyped":"0",
    "s":"4007",
    "page":"1"
}
headers = {
    "Referer":"http://hotels.ctrip.com/hotel/shanghai2",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Host":"hotels.ctrip.com"
}
class Location_XC:
    def __init__(self):
        self.conn = pymssql.connect(host='119.145.8.188:16433', user='sa', password='Ecaim6688', database='HotelSpider')
        self.cur = self.conn.cursor()
        self.q = queue.Queue()
    def __del__(self):
        self.cur.close()
        self.conn.close()
    def loc_scenis(self,cityid):
        # if cityid == "34":
        #     return
        data = {
            "city":cityid,
            "markType":"4",
            "newversion":"T"
        }
        response_location = requests.post("http://hotels.ctrip.com/Domestic/Tool/AjaxShowMoreDiv.aspx",headers=city_headers,data=data)
        with open("scenis.html","w",encoding="utf-8") as f:
            f.write(response_location.content.decode())
        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                location_list = HTML_location.xpath(".//div[@title]/text()")
                # print(location_list)
                # if location_list != []:
                #     self.insert_scenis(cityid,location_list)
                for lacation in location_list:
                    ind = location_list.index(lacation)
                    sl_id = HTML_location.xpath(".//div['%s']/@data-value" %ind)[0]
                    self.update_scenis(cityid,lacation,sl_id)


    def update_scenis(self,cityid,title,sl):
        data["cityId"] = cityid
        data["s"] = ''
        data["sl"] = sl

        response = requests.post(url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx", data=data,
                                 headers=headers)

        try:
            json_response = json.loads(response.content.decode())
        except Exception as e:
            ret = re.sub(r"\w+\\\w+",'',response.content.decode())
            json_response = json.loads(ret)

        else:
            street = json_response["landmarks"][0]
            lng, lat = street["position"].split("|")

            str_date = (str(cityid) + str(title)).encode("utf-8")
            SId = hashlib.md5(str_date).hexdigest()

            # sql = "update Scenic set lat='%f',lng='%f' where SId='%s'" % (float(lat), float(lng), str(StaId))
            sql = "insert into Scenic (SId,CId,scenic,lat,lng) values ('%s','%s','%s','%f','%f')" %(SId,cityid,title,float(lat),float(lng))
            print(sql)
            try:
                self.cur.execute(sql)
            except Exception as e:
                print(e)
            self.conn.commit()

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
        data = {
            "__VIEWSTATEGENERATOR": "DB1FBB6D",
            "StartTime": "2018-07-03",
            "DepTime": "2018-07-04",
            "operationtype": "NEWHOTELORDER",
            "cityId": cityid,
            "OrderBy": "99",
            "checkIn": "2018-07-03",
            "checkOut": "2018-07-04",
            "hidTestLat": "0%7C0",
            "isfromlist": "T",
            "ubt_price_key": "htl_search_result_promotion",
            "htlFrom": "hotellist",
            "markType": "0",
            "a": "0",
            "contrast": "0",
            "attachDistance": "0",
            "contyped": "0",
            "s": value,
            "page": "1"
        }
        headers = {
            "Referer": "http://hotels.ctrip.com/hotel/shanghai2",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
            "Host": "hotels.ctrip.com"
        }
        response = requests.post(url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx", data=data,
                                 headers=headers)
        try:
            json_response = json.loads(response.content.decode())
        except Exception as e:
            with open("sub_err.txt","a",encoding="utf-8") as f:
                f.write(cityid+title+value+"\r\n")
        else:
            street = json_response["landmarks"][0]
            lat,lng = street["position"].split("|")

            str_date = (str(cityid) + str(line) + str(title)).encode("utf-8")
            SId = hashlib.md5(str_date).hexdigest()

            sql = "update Subway set lat='%f',lng='%f' where SId='%s'" % (float(lat),float(lng),SId)
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

        if response_location.status_code == 200:
            HTML_location = etree.HTML(response_location.content.decode())
            if HTML_location is not None:
                location_list = HTML_location.xpath(".//div[@id]/text()")
                # print(location_list)
                # if location_list != []:
                    # self.insert_station(cityid, location_list)
                for location in location_list:
                    sl_id = HTML_location.xpath(".//div[text()='%s']/@data-value" %location)[0]
                    self.update_station(cityid,location,sl_id)
    def update_station(self,cityid,location,sl):
        data["cityId"] = cityid
        data["s"] = ''
        data["sl"] = sl

        response = requests.post(url="http://hotels.ctrip.com/Domestic/Tool/AjaxHotelList.aspx", data=data,
                                 headers=headers)

        try:
            json_response = json.loads(response.content.decode())
        except Exception as e:
            with open("sta_err.txt","a",encoding="utf-8") as f:
                f.write(cityid+location+sl+"\r\n")
        else:
            street = json_response["landmarks"][0]
            lat, lng = street["position"].split("|")

            str_date = (str(cityid) + str(location)).encode("utf-8")
            StaId = hashlib.md5(str_date).hexdigest()

            sql = "update Station set lat='%f',lng='%f' where StaId='%s'" %(float(lat),float(lng),str(StaId))
            try:
                self.cur.execute(sql)
            except Exception as e:
                pass
            self.conn.commit()
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

    def elong_lacation(self,city):
        city_PY = city[0].replace("'",'').replace(" ",'')
        city_id = city[2]
        city_kw = city[1]
        url = "http://hotel.elong.com/{}/".format(city_PY)
        headers = {
            "Referer":"http://www.elong.com/",
            "Host":"hotel.elong.com",
            "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
        }
        response = requests.get(url,headers=headers)
        html = etree.HTML(response.content.decode("utf-8"))
        if "您访问的页面不存在或暂时无法访问" in response.content.decode():
            redis_server.lpush("notcity",city)
        hospital_type = html.xpath(".//li[text()='\n医院']/@data-typeid")[0] if html.xpath(".//li[text()='\n医院']/@data-typeid") != [] else None
        school_type = html.xpath(".//li[text()='\n大学']/@data-typeid")[0] if html.xpath(".//li[text()='\n大学']/@data-typeid") != [] else None
        station_type = html.xpath(".//li[text()='\n机场/车站']/@data-typeid")[0] if html.xpath(".//li[text()='\n机场/车站']/@data-typeid") != [] else None
        stadium_type = html.xpath(".//li[text()='\n演出场馆']/@data-typeid")[0] if html.xpath(".//li[text()='\n演出场馆']/@data-typeid") != [] else None


        hospitals_name = html.xpath(".//ul[@data-typeid='%s']/li/@title" %hospital_type)
        hospitals_value = html.xpath(".//ul[@data-typeid='%s']/li/@data-id" %hospital_type)

        schools_name = html.xpath(".//ul[@data-typeid='%s']/li/@title" %school_type)
        schools_value = html.xpath(".//ul[@data-typeid='%s']/li/@data-id" %school_type)

        stadiums_name = html.xpath(".//ul[@data-typeid='%s']/li/@title" % stadium_type)
        stadiums_value = html.xpath(".//ul[@data-typeid='%s']/li/@data-id" % stadium_type)

        if school_type:
            self.insert_school(schools_name,schools_value,city_id,city_kw)
        if hospital_type:
            self.insert_hospital(hospitals_name,hospitals_value,city_id,city_kw)
        if stadium_type:
            self.insert_stadium(stadiums_name,stadiums_value,city_id,city_kw)

    def insert_hospital(self,names,ids,cid,city_kw):
        sql = "insert into Hospital (HospId,CId,Name,Lat,Lng) values "
        for name in names:
            lat,lng = self.other_position(name,city_kw)
            ind = names.index(name)
            id = ids[ind]
            sql = sql + "('%s','%s','%s','%f','%f')" %(id,cid,str(name),float(lat),float(lng))
        sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except:
            pass
        self.conn.commit()

    def insert_school(self,names,ids,cid,city_kw):
        sql = "insert into School (SchId,CId,Name,Lat,Lng) values "
        for name in names:
            lat, lng = self.other_position(name,city_kw)
            ind = names.index(name)
            id = ids[ind]
            sql = sql + "('%s','%s','%s','%f','%f')" %(id,cid,str(name),float(lat),float(lng))
        sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except Exception as e:
            pass
        self.conn.commit()

    def insert_stadium(self,names,ids,cid,city_kw):
        sql = "insert into Stadium (StaId,CId,Name,Lat,Lng) values "
        for name in names:
            lat, lng = self.other_position(name,city_kw)
            ind = names.index(name)
            id = ids[ind]
            sql = sql + "('%s','%s','%s','%f','%f')" %(id,cid,str(name),float(lat),float(lng))
        sql = sql.replace(')(', '),(')
        try:
            self.cur.execute(sql)
        except:
            pass
        self.conn.commit()

    def other_position(self,keyword,city_kw):
        url = "http://apis.map.qq.com/jsapi?qt=poi&wd={keyword}&pn=0&rn=10&rich_source=qipao&rich=web&nj=0&c=1&key=FBOBZ-VODWU-C7SVF-B2BDI-UK3JE-YBFUS&output=jsonp&pf=jsapi&ref=jsapi&cb=qq.maps._svcb3.search_service_0"
        url2 = "http://restapi.amap.com/v3/place/text?s=rsv3&children=&key=169d2dd7829fe45690fabec812d05bc3&offset=10&page=1&language=zh_cn&callback=jsonp_587969_&platform=JS&logversion=2.0&sdkversion=1.3&appname=http%3A%2F%2Fwww.gpsspg.com%2Fiframe%2Fmaps%2Famap_161128.htm%3Fmapi%3D3&csid=5BA24A15-D366-4B8E-9A4B-ED74E012696B&keywords={kw}"

        headers = {
            "Referer":"http://www.gpsspg.com/iframe/maps/qq_161128.htm?mapi=2",
            "User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
        }
        response = requests.get(url.format(keyword=keyword),headers=headers)

        json_resp = json.loads(response.content.decode("GBK")[67:-1])
        try:
            lng = json_resp["detail"]["pois"][0]["pointx"]
            lat = json_resp["detail"]["pois"][0]["pointy"]
        except Exception as e:
            response = requests.get(url2.format(kw=(str(city_kw)+str(keyword))),headers=headers)
            rep_json = json.loads(response.content.decode()[14:-1])
            try:
                location1 = rep_json["pois"][0]["location"]
            except Exception as e:
                response = requests.get(url.format(keyword=str(city_kw)+str(keyword)),headers=headers)
                json_resp = json.loads(response.content.decode("GBK")[67:-1])
                try:
                    lng = json_resp["detail"]["pois"][0]["pointx"]
                    lat = json_resp["detail"]["pois"][0]["pointy"]
                except:
                    lat = 0
                    lng = 0
            else:
                lng,lat = location1.split(",")
        return lat,lng

    def run(self):

        city_response = requests.get("http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx", headers=city_headers)
        ret = re.findall(r'data:"(.*?)"', city_response.content.decode())
        ret = set(ret)
        ret_hot = re.findall(r'suggestion={热门:\[(.*?)\],AB',city_response.content.decode())[0]
        ret_hot = re.findall(r'data:"(.*?)",',ret_hot)
        # yet_ret = self.select_scen()
        # all_ret = []
        # print(yet_ret)
        # noli = [["qionghai_boao","琼海","52"],["qiqihaer","齐齐哈尔","149"],["hulunbeier","呼伦贝尔","4255"]]
        for i in ret:
            i.replace("/",'')
            ret2 = i.split("|")
            # ret2.pop(1)
            print(ret2)
            self.elong_lacation(ret2)
            # all_ret.append(ret2[1])
            # self.loc_business(ret2[1])
            # self.loc_station(ret2[1])
            # self.loc_subway(ret2[1])
            # self.loc_admin(ret2[1])
            # self.loc_scenis("492")
            sleep(1)
        # self.other_position("文登师范学校")
        # self.select_scen()
    def select_scen(self):
        # 查询剩余 并添加到redis
        sql = "select name,cid from City where Cname not in (select cid from OtherFacility group by cid)"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        import numpy
        li = numpy.array(ret)
        # print(len(li))
        for i in li:
            # ret = str(i[0]).replace(" ",'')
            i = list(i)
            print(i)
            redis_server.lpush("city",i)
            # self.loc_scenis()
        # return list1
if __name__ == '__main__':
    location = Location_XC()
    location.run()
    # location.select_scen()
