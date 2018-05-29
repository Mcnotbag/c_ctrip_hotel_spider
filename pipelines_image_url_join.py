# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import time
from pprint import pprint
import pymssql

class XiechengPipeline(object):
    def __init__(self):
        self.conn = pymssql.connect(host='192.168.2.135\sql2008', user='sa', password='sa', database='HotelSpider')
        self.cur = self.conn.cursor()

    def close_spider(self,spider):
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        item["Roomtype"]["Rarea"] = item["Roomtype"]["Rarea"][1] if item["Roomtype"]["Rarea"] != [] else []
        # 清洗设施
        item["RoomFacility"] = [i.replace(" ","").replace("\n","") for i in item["RoomFacility"]] if item["RoomFacility"] != [] else []
        while '' in item["RoomFacility"]:
            item["RoomFacility"].remove('')
        item["RoomFacility"] = [str(i) for i in item["RoomFacility"]]
        item["RoomFacility"] = ''.join(item["RoomFacility"])

        # 清洗bed
        item["Roomtype"]["Rbed"] = [i.replace(" ","").replace("\n","") for i in item["Roomtype"]["Rbed"]][1] if item["Roomtype"]["Rbed"] != [] else []
        # 清洗Rfloor
        item["Roomtype"]["Rfloor"] = [i.replace(" ","").replace("\n","") for i in item["Roomtype"]["Rfloor"]][1] if item["Roomtype"]["Rfloor"] != [] else []
        # 清洗Rtitle
        item["Roomtype"]["Rtitle"] = [i.replace(" ","").replace("\n","") for i in item["Roomtype"]["Rtitle"]][0].replace("\t","") if item["Roomtype"]["Rtitle"] != [] else []

        # 清洗RId
        # item["Roomtype"]["RId"] = item["Roomtype"]["RId"][0].replace("rdn","") if item["Roomtype"]["RId"] != [] else []

        #清洗电话
        if item["Phone"]:
            item["Phone"] = item["Phone"][0].split(" / ") if "/" in item["Phone"][0] else item["Phone"][0]
            if ',' in str(item["Phone"]):
                phone_list = eval(str(item["Phone"]))
                item["Phone"] = ';'.join(phone_list)
        # 清洗开业时间
        item["KYdate"] = item["KYdate"][0] if item["KYdate"] != [] else []
        item["ZXdate"] = item["ZXdate"][0] if item["ZXdate"] != [] else ''

        # 清洗房间总数
        item["Roomtotal"] = item["Roomtotal"][0] if item["Roomtotal"] != [] else ''

        # 清洗经纬度
        item["Latitude"] = item["street"].split("|")[1] if item["street"] != [] else []
        item["Longitude"] = item["street"].split("|")[0] if item["street"] != [] else []
        # 清洗酒店介绍
        item["Description"] = item["Description"][0].replace("<br>","").replace("\u3000","") if item["Description"] != [] else []

        # 清洗有无早餐
        item["Roomtype"]["room"]["breakfast"] = item["Roomtype"]["room"]["breakfast"][0] if item["Roomtype"]["room"]["breakfast"] != [] else "有早"

        # 清洗PId
        item["Roomtype"]["room"]["PId"] = item["Roomtype"]["room"]["PId"][0] if item["Roomtype"]["room"]["PId"] != [] else []

        # 清洗price
        item["Roomtype"]["room"]["price"] = item["Roomtype"]["room"]["price"][0] if item["Roomtype"]["room"]["price"] != [] else 0
        item["Roomtype"]["room"]["price"] = str(item["Roomtype"]["room"]["price"]).replace("\t",'').replace(' ','')

        #清洗people
        item["Roomtype"]["room"]["people"] = item["Roomtype"]["room"]["people"][7] if len(item["Roomtype"]["room"]["people"]) == 8 else 2

        #清洗评分
        item["Score"] = str(item["Score"]).replace("\t",'').replace(' ','')


        # 插入数据库
        """cursor.executemany(
            "INSERT INTO persons VALUES (%d, %s, %s)",
            [(1, 'John Smith', 'John Doe'),
             (2, 'Jane Doe', 'Joe Dog'),
             (3, 'Mike T.', 'Sarah H.')])"""
        """conn.execute_non_query("INSERT INTO persons VALUES(1, 'John Doe')")"""

        # 逻辑先判断有没有这个数据,有的话更新，没有插入
        hotel_ret = self.select_hotel(item["HId"])
        if hotel_ret:
            # update Hotel表
            self.update_hotel(item)
        else:
            # insert Hotel表中的数据
            self.insert_hotel(item)
        image_ret = self.select_image(item["Roomtype"]["RId"])
        if image_ret:
            # update image表
            self.update_image(item)
        else:
            # insert Image表数据
            self.insert_image(item)
        room_ret = self.select_room(item["Roomtype"]["RId"])
        if room_ret:
            # update room表
            self.update_room(item)
        else:
            # insert Room表数据
            self.insert_room(item)
        price_ret = self.select_price(str(item["Roomtype"]["room"]["PId"]))
        if price_ret:
            # update price表
            self.update_price(item)
        else:
            # insert Price表数据
            self.insert_price(item)

        return item

    # 插入数据库
    def insert_hotel(self,item):
        # now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print(str(item["Phone"]))
        insert = "INSERT INTO Hotel (Source, HId, City, Name, Cover, [Level], Score, Address, Price, Phone, KYDate," \
                 + "RoomCount, ZXDate, Latitude, Longitude, Url, Description) values ('%d','%s','%s','%s','%s','%s','%f','%s','%.2f','%s','%s','%s','%s','%f','%f','%s','%s')" %(
            int((item["Source"])),item["HId"],str(item["City"]),str(item["Name"]),str(item["Cover"]),str(item["Star"]),float(item["Score"]),str(item["Address"]),float(item["Roomtype"]["room"]["price"]),str(item["Phone"]),str(item["KYdate"]),\
            str(item["Roomtotal"]),str(item["ZXdate"]),float(item["Latitude"]),float(item["Longitude"]),str(item["HUrl"]),str(item["Description"])
        )
        try:
            self.cur.execute(insert)
            print("插入成功Hotel")
        except Exception as e:
            print("插入失败Hotel")
            print(e)
        self.conn.commit()

    def insert_image(self,item):
        if len(item["Roomtype"]["Rimage"]) > 1:
            image_str = ';'.join(item["Roomtype"]["Rimage"])
        else:
            image_str = item["Roomtype"]["Rimage"][0] if item["Roomtype"]["Rimage"] != [] else ''
        insert = "INSERT INTO Image (HId, RId, Url) VALUES ('%s','%s','%s')" % (str(item["HId"]), str(item["Roomtype"]["RId"]),str(image_str))

        if image_str != '':
            try:
                self.cur.execute(insert)
                # print("插入成功Image")
            except Exception as e:
                print(e)
                print("插入失败Image")
        self.conn.commit()

    def insert_room(self,item):
        # float(min(item["Roomtype"]["type"]["price"]))

        insert = "INSERT INTO Room (Source, HId, RId, Cover, Name, Floor, Area, Price, People, Bed) VALUES ('%d','%d','%d','%s','%s','%s','%s','%.2f','%d','%s')" %(
            int((item["Source"])),int(item["HId"]),int(item["Roomtype"]["RId"]),str(item["Cover"]),str(item["Roomtype"]["Rtitle"]),
            str(item["Roomtype"]["Rfloor"]),str(item["Roomtype"]["Rarea"]),float(item["Roomtype"]["room"]["price"]),int(item["Roomtype"]["room"]["people"]),str(item["Roomtype"]["Rbed"])
        )


        try:
            self.cur.execute(insert)
            # print("插入成功Room")
        except Exception as e:
            print(e)
            print("插入失败Room")
        self.conn.commit()
        # del item["Roomtype"]["type"]["price"][0]

    def insert_price(self,item):

        insert = "INSERT INTO Price (Source, HId, RId, PId, Name, Meal, Price) VALUES ('%d','%s','%s','%s','%s','%s','%.2f')" %(
            int((item["Source"])),str(item["HId"]),str(item["Roomtype"]["RId"]),str(item["Roomtype"]["room"]["PId"]),str(item["Roomtype"]["Rtitle"]),str(item["Roomtype"]["room"]["breakfast"]), float(item["Roomtype"]["room"]["price"])
        )

        try:
            self.cur.execute(insert)
            # print("插入成功Price")
        except Exception as e:
            print(e)
            print("插入失败Price")
        self.conn.commit()

    # 查询数据库
    def select_hotel(self,HId):
        select = "select count(*) from Hotel where HId='%s'" % HId

        ret = None
        try:
            self.cur.execute(select)
            ret = self.cur.fetchone()
            # print("查询成功Hotel")
        except Exception as e:
            print("查询失败Hotel")
            print(e)
        self.conn.commit()

        if ret[0]:
            return 1
        else:
            return 0

    def select_room(self,RId):
        select = "select count(*) from Room where RId='%s'" % RId

        ret = None
        try:
            self.cur.execute(select)
            ret = self.cur.fetchone()
            # print("查询成功Room")
        except Exception as e:
            print("查询失败Room")
            print(e)
        self.conn.commit()

        if ret[0]:
            return 1
        else:
            return 0

    def select_image(self,RId):
        select = "select count(*) from Image where RId='%s'" % RId
        ret = None

        try:
            self.cur.execute(select)
            ret = self.cur.fetchone()
            # print("查询成功Image")
        except Exception as e:
            print("查询失败Image")
            print(e)
        self.conn.commit()

        if ret[0]:
            return 1
        else:
            return 0

    def select_price(self,PId):
        select = "select count(*) from Price where PId='%s'" % PId
        ret = None
        try:
            self.cur.execute(select)
            ret = self.cur.fetchone()
            # print("查询成功Price")
        except Exception as e:
            print("查询失败Price")
            print(e)
        self.conn.commit()

        if ret[0]:
            return 1
        else:
            return 0

    # 更新数据库
    def update_hotel(self,item):

        update = "update Hotel set Score='%f',Price='%.2f',Phone='%s',ZXDate='%s',RoomCount='%s' where HId='%s'" %(
            float(str(item["Score"])),float(item["Roomtype"]["room"]["price"]),str(item["Phone"]),str(item["ZXdate"]),str(item["Roomtotal"]),str(item["HId"])
        )
        try:
            self.cur.execute(update)
            # print("更新成功Hotel")
            self.conn.commit()
        except Exception as e:
            print("更新失败Hotel")
            print(str(item["Phone"]))
            print(e)
            self.conn.rollback()

    def update_room(self,item):
        update = "update Room set Name='%s',Price='%.2f' where RId='%s'" %(str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),int(item["Roomtype"]["RId"]))
        try:
            self.cur.execute(update)
            # print("更新成功Room")
            self.conn.commit()
        except Exception as e:
            print("更新失败Room")
            print(e)
            self.conn.rollback()

    def update_image(self,item):
        if len(item["Roomtype"]["Rimage"]) > 1:
            image_str = ';'.join(item["Roomtype"]["Rimage"])
        else:
            image_str = item["Roomtype"]["Rimage"][0] if item["Roomtype"]["Rimage"] != [] else ''

        update = "update Image set Url='%s' where RId='%s'" %(str(image_str),int(item["Roomtype"]["RId"]))
        if image_str != '':
            try:
                self.cur.execute(update)
                # print("更新成功Image")
                self.conn.commit()
            except Exception as e:
                print("更新失败Image")
                print("*"*50)
                print(len(image_str))
                print(e)
                self.conn.rollback()

    def update_price(self,item):
        update = "update Price set Name='%s',Price='%.2f' where PId='%s'" %(str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),str(item["Roomtype"]["room"]["PId"]))
        try:
            self.cur.execute(update)
            # print("更新成功Price")
            self.conn.commit()
        except Exception as e:
            print("更新失败Price")
            print(e)
            self.conn.rollback()




