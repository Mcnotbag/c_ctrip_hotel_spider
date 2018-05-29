import json
import pymssql
import re
import requests
from redis import Redis


class City(object):
    def __init__(self):
        self.conn = pymssql.connect(host='192.168.2.135\sql2008', user='sa', password='sa', database='HotelSpider')
        self.cur = self.conn.cursor()
        self.redis_server = Redis(host="111.230.34.217", port=6379, decode_responses=True)

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def reqeust_city(self):

        city_headers = {
            'Referer': 'http://hotels.ctrip.com/hotel',
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"
        }
        city_response = requests.get("http://hotels.ctrip.com/Domestic/Tool/AjaxGetCitySuggestion.aspx", headers=city_headers)
        ret = re.findall(r'data:"(.*?)"', city_response.content.decode())
        ret_hot = re.findall(r'suggestion={热门:\[(.*?)\],AB',city_response.content.decode())[0]
        ret_hot = re.findall(r'data:"(.*?)",',ret_hot)
        # print(ret_hot)
        print(len(ret))
        print(len(ret_hot))
        return ret_hot,ret

    def insert_city_hot(self,ret_hot):
        for hot in ret_hot:
            city = hot.split("|")
            if "'" in city[0]:
                city[0] = city[0].replace("'",'')
            if " " in city[0]:
                city[0] = city[0].replace(" ",'')
            insert = "INSERT INTO City (Source, CId, Name, Hot, Status, Zname) VALUES ('%d', '%s', '%s', '%d', '%d', '%s')" %(int(1),str(city[2]),str(city[0]),1,0,str(city[1]))
            try:
                self.cur.execute(insert)
                # print("插入热门城市成功-%s" %city[0])
            except Exception as e:
                print(e)
                print("插入热门城市失败-%s" %city[0])
            self.conn.commit()

    def insert_city_all(self,ret_all):
        for city in ret_all:
            city = city.split("|")
            if "'" in city[0]:
                city[0] = city[0].replace("'",'')
            if " " in city[0]:
                city[0] = city[0].replace(" ",'')
            insert = "insert into City(Source, CId, Name, Hot, Status, Zname) values ('%d', '%s', '%s', '%d', '%d','%s')" %(int(1),str(city[2]),str(city[0]),0,0,str(city[1]))
            try:
                self.cur.execute(insert)
                # print("插入城市成功-%s" %city[0])
            except Exception as e:
                print(e)
                print("插入城市失败-%s" %city[0])
            self.conn.commit()

    def insert_redis_hot_city(self,ret_hot):
        for city in ret_hot:
            city = city.split("|")
            if "'" in city[0]:
                city[0] = city[0].replace("'",'')
            if " " in city[0]:
                city[0] = city[0].replace(" ",'')

    def run(self):
        ret_hot,ret = self.reqeust_city()
        # self.insert_city_hot(ret_hot)
        # self.insert_city_all(ret)
        print(ret_hot)




if __name__ == '__main__':
    city = City()
    city.run()

