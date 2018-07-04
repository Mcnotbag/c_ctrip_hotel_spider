# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import hashlib
import time
from pprint import pprint
import pymssql

class XiechengPipeline(object):
    def __init__(self):
        self.conn = pymssql.connect(host='119.145.8.188:16433', user='sa', password='Ecaim6688', database='HotelSpider')
        self.cur = self.conn.cursor()
    def close_spider(self,spider):
        self.cur.close()
        self.conn.close()

    def process_item(self, item, spider):
        if spider.name == 'XC':
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
            # item["Roomtype"]["Rtitle"] = [i.replace(" ","").replace("\n","") for i in item["Roomtype"]["Rtitle"]][0].replace("\t","") if item["Roomtype"]["Rtitle"] != [] else []

            # 清洗RId
            # item["Roomtype"]["RId"] = item["Roomtype"]["RId"][0].replace("rdn","") if item["Roomtype"]["RId"] != [] else []

            #清洗电话
            if item["Phone"]:
                item["Phone"] = item["Phone"][0].split(" / ") if "/" in item["Phone"][0] else item["Phone"][0]
                if ',' in str(item["Phone"]):
                    phone_list = eval(str(item["Phone"]))
                    item["Phone"] = ';'.join(phone_list)
                if '/' in str(item["Phone"]):
                    phone_list = eval(str(item["Phone"]))
                    item["Phone"] = ';'.join(phone_list)
            # 清洗开业时间
            item["KYdate"] = item["KYdate"][0] if item["KYdate"] != [] else []
            item["ZXdate"] = item["ZXdate"][0] if item["ZXdate"] != [] else ''

            # 清洗房间总数
            item["Roomtotal"] = item["Roomtotal"][0] if item["Roomtotal"] != [] else ''

            # 清洗经纬度
            item["Latitude"] = item["street"].split("|")[1] if item["street"] != '' else 0
            item["Longitude"] = item["street"].split("|")[0] if item["street"] != '' else 0
            # 清洗酒店介绍
            item["Description"] = item["Description"][0].replace("<br>","").replace("\u3000","").replace("'",'') if item["Description"] != [] else []

            # 清洗有无早餐
            item["Roomtype"]["room"]["breakfast"] = item["Roomtype"]["room"]["breakfast"][0] if item["Roomtype"]["room"]["breakfast"] != [] else "有早"

            # 清洗PId
            item["Roomtype"]["room"]["PId"] = item["Roomtype"]["room"]["PId"][0] if item["Roomtype"]["room"]["PId"] != [] else []

            # 清洗price
            item["Roomtype"]["room"]["price"] = item["Roomtype"]["room"]["price"][0] if item["Roomtype"]["room"]["price"] != [] else 0
            item["Roomtype"]["room"]["price"] = str(item["Roomtype"]["room"]["price"]).replace("\t",'').replace(' ','')
            item["index_price"] = str(item["index_price"]).replace("\t",'').replace(' ','')

            #清洗people
            item["Roomtype"]["room"]["people"] = item["Roomtype"]["room"]["people"][7] if len(item["Roomtype"]["room"]["people"]) == 8 else 2

            #清洗评分
            item["Score"] = str(item["Score"]).replace("\t",'').replace(' ','')

            # 清洗商业圈
            item["business"] = ','.join(item["business"])
            #清洗区域 area
            item["Area"] = item["Area"][0] if item["Area"] != [] else ''
            # 清洗Hlabel
            item["Hlabel"] = ','.join(item["Hlabel"])
            # 插入数据库
            """cursor.executemany(
                "INSERT INTO persons VALUES (%d, %s, %s)",
                [(1, 'John Smith', 'John Doe'),
                 (2, 'Jane Doe', 'Joe Dog'),
                 (3, 'Mike T.', 'Sarah H.')])"""
            """conn.execute_non_query("INSERT INTO persons VALUES(1, 'John Doe')")"""

            self.unite_sql_hotel(item)
            self.unite_sql_room(item)
            self.unite_sql_price(item)
            # for image in item["Roomtype"]["Rimage"]:
            self.insert_image(item)
            self.insert_Hfacility(item)
            self.insert_Ofacility(item)
            self.insert_Allimage(item)
            return item
        elif spider.name == 'relation':
            print(item)

    # 插入数据库
    def insert_hotel(self,item):
        # now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        # print(str(item["Phone"]))
        insert = "INSERT INTO Hotel (Source, HId, City, Name, Cover, [Level], Score, Address, Price, Phone, KYDate," \
                 + "RoomCount, ZXDate, Latitude, Longitude, Url, Description) values ('%d','%s','%s','%s','%s','%s','%f','%s','%.2f','%s','%s','%s','%s','%f','%f','%s','%s')" %(
            int((item["Source"])),item["HId"],str(item["City"]),str(item["Name"]),str(item["Cover"]),str(item["Star"]),float(item["Score"]),str(item["Address"]),float(item["index_price"]),str(item["Phone"]),str(item["KYdate"]),\
            str(item["Roomtotal"]),str(item["ZXdate"]),float(item["Latitude"]),float(item["Longitude"]),str(item["HUrl"]),str(item["Description"])
        )
        try:
            self.cur.execute(insert)
            print("插入成功Hotel")
            self.conn.commit()
        except Exception as e:
            # print("插入失败Hotel")
            self.update_hotel(item)

    def insert_image(self,item):
        # if len(item["Roomtype"]["Rimage"]) > 1:
        #     image_str = ';'.join(item["Roomtype"]["Rimage"])
        # else:
        #     image_str = item["Roomtype"]["Rimage"][0] if item["Roomtype"]["Rimage"] != [] else ''
        if item["Roomtype"]["Rimage"]:
            insert = "INSERT INTO Image (HId, RId, Url) VALUES ('%s','%s','%s')"

            for image in item["Roomtype"]["Rimage"]:
                insert = insert + "('%s','%s','%s')" % (str(item["HId"]), str(item["Roomtype"]["RId"]),str(image))
            insert = insert.replace(')(','),(')
            try:
                self.cur.execute(insert)
                # print("插入成功Image")
            except Exception as e:
                # print(e)
                # print("插入失败Image")
                pass
            self.conn.commit()

    def insert_room(self,item):
        # float(min(item["Roomtype"]["type"]["price"]))

        insert = "INSERT INTO Room (Source, HId, RId, Cover, Name, Floor, Area, Price, People, Bed) VALUES ('%d','%s','%s','%s','%s','%s','%s','%.2f','%d','%s')" %(
            int((item["Source"])),str(item["HId"]),str(item["Roomtype"]["RId"]),str(item["Roomtype"]["Rcover"]),str(item["Roomtype"]["Rtitle"]),
            str(item["Roomtype"]["Rfloor"]),str(item["Roomtype"]["Rarea"]),float(item["Roomtype"]["room"]["price"]),int(item["Roomtype"]["room"]["people"]),str(item["Roomtype"]["Rbed"])
        )


        try:
            self.cur.execute(insert)
            # print("插入成功Room")
        except Exception as e:
            # print("插入失败Room")
            self.update_room(item)
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
            # print("插入失败Price")
            self.update_price(item)
        self.conn.commit()

    # 查询数据库
    def select_hotel(self,HId):
        select = "select count(Id) from Hotel where HId='%s'" % HId

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
        select = "select count(Id) from Room where RId='%s'" % RId

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

    def select_image(self,RId,image):
        select = "select count(Id) from Image where RId='%s' and Url='%s'" % (RId,image)
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
        select = "select count(Id) from Price where PId='%s'" % PId
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

        update = "update Hotel set Score='%f',Price='%.2f',Phone='%s',ZXDate='%s',RoomCount='%s',UpdateTime='%s' where HId='%s'" %(
            float(str(item["Score"])),float(item["index_price"]),str(item["Phone"]),str(item["ZXdate"]),str(item["Roomtotal"]),str(datetime.datetime.now())[:23],str(item["HId"])
        )
        try:
            self.cur.execute(update)
            # print("更新成功Hotel")
            self.conn.commit()
        except Exception as e:
            print("更新失败Hotel")
            print(str(item["Phone"]))
            print(e)

    def update_room(self,item):
        update = "update Room set Cover='%s',Name='%s',Price='%.2f',UpdateTime='%s' where RId='%s'" %(str(item["Roomtype"]["Rcover"]),str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),str(datetime.datetime.now())[:23],str(item["Roomtype"]["RId"]))
        try:
            self.cur.execute(update)
            # print("更新成功Room")
            # self.conn.commit()
        except Exception as e:
            print("更新失败Room")
            # print(e)
            # self.conn.rollback()

    def update_image(self,item,image):


        update = "update Image set Url='%s' where RId='%s'" %(str(image),int(item["Roomtype"]["RId"]))
        try:
            self.cur.execute(update)
            # print("更新成功Image")
            # self.conn.commit()
        except Exception as e:
            print("更新失败Image")
            # print("*"*50)
            # print(len(image))
            # print(e)
            # self.conn.rollback()

    def update_price(self,item):
        update = "update Price set Name='%s',Price='%.2f',UpdateTime='%s' where PId='%s'" %(str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),str(datetime.datetime.now())[:23],str(item["Roomtype"]["room"]["PId"]))
        try:
            self.cur.execute(update)
            # print("更新成功Price")
            # self.conn.commit()
        except Exception as e:
            print("更新失败Price")
            # print(e)
            # self.conn.rollback()


    def unite_sql_hotel(self,item):

        sql = "if exists(select top 1 * from HotelSpider.dbo.Hotel where HId = '%s')" %str(item["HId"]) + \
              " begin update Hotel set Score='%f',Price='%.2f',Phone='%s',ZXDate='%s',RoomCount='%s',UpdateTime='%s',area='%s',business='%s',label='%s' where HId='%s' end" %(
            float(str(item["Score"])),float(item["index_price"]),str(item["Phone"]),str(item["ZXdate"]),str(item["Roomtotal"]),str(datetime.datetime.now())[:23],str(item["Area"]),str(item["business"]),str(item["Hlabel"]),str(item["HId"])
        ) + \
              " else begin INSERT INTO Hotel (Source, HId, City, Name, Cover, [Level], Score, Address, Price, Phone, KYDate," \
                 + "RoomCount, ZXDate, Latitude, Longitude, Url, Description, area, business,label) values ('%d','%s','%s','%s','%s','%s','%f','%s','%.2f','%s','%s','%s','%s','%f','%f','%s','%s','%s','%s','%s') end" %(
            int((item["Source"])),item["HId"],str(item["City"]),str(item["Name"]),str(item["Cover"]),str(item["Star"]),float(item["Score"]),str(item["Address"]),float(item["index_price"]),str(item["Phone"]),str(item["KYdate"]),\
            str(item["Roomtotal"]),str(item["ZXdate"]),float(item["Latitude"]),float(item["Longitude"]),str(item["HUrl"]),str(item["Description"]),str(item["Area"]),str(item["business"]),str(item["Hlabel"]))

        try:
            self.cur.execute(sql)
            # print("执行成功hotel")
        except Exception as e:
            print("执行失败hotel")
            print(e)
            pprint(item)
        self.conn.commit()

    def unite_sql_room(self,item):
        sql = "if exists(select top 1 * from HotelSpider.dbo.Room where RId = '%s')" % str(item["Roomtype"]["RId"]) + \
              " begin update Room set Cover='%s',Name='%s',Price='%.2f',UpdateTime='%s' where RId='%s' end" %(str(item["Roomtype"]["Rcover"]),str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),
                                                                                                              str(datetime.datetime.now())[:23],str(item["Roomtype"]["RId"]))+ \
              " else begin INSERT INTO Room (Source, HId, RId, Cover, Name, Floor, Area, Price, People, Bed) VALUES ('%d','%s','%s','%s','%s','%s','%s','%.2f','%d','%s') end" %(
            int((item["Source"])),str(item["HId"]),str(item["Roomtype"]["RId"]),str(item["Roomtype"]["Rcover"]),str(item["Roomtype"]["Rtitle"]),
            str(item["Roomtype"]["Rfloor"]),str(item["Roomtype"]["Rarea"]),float(item["Roomtype"]["room"]["price"]),int(item["Roomtype"]["room"]["people"]),str(item["Roomtype"]["Rbed"])
        )
        try:
            self.cur.execute(sql)
        except Exception as e:
            print("执行失败room")
            print(e)
        self.conn.commit()

    def unite_sql_price(self,item):
        sql = "if exists(select top 1 * from HotelSpider.dbo.Price where PId = '%s')" %str(item["Roomtype"]["room"]["PId"]) + \
              " begin update Price set Name='%s',Price='%.2f',UpdateTime='%s' where PId='%s' end" %(str(item["Roomtype"]["Rtitle"]),float(item["Roomtype"]["room"]["price"]),
                                                                                                    str(datetime.datetime.now())[:23],str(item["Roomtype"]["room"]["PId"]))+ \
              " else begin INSERT INTO Price (Source, HId, RId, PId, Name, Meal, Price) VALUES ('%d','%s','%s','%s','%s','%s','%.2f') end" %(
            int((item["Source"])),str(item["HId"]),str(item["Roomtype"]["RId"]),str(item["Roomtype"]["room"]["PId"]),str(item["Roomtype"]["Rtitle"]),str(item["Roomtype"]["room"]["breakfast"]), float(item["Roomtype"]["room"]["price"])
        )

        try:
            self.cur.execute(sql)
        except Exception as e:
            print("执行失败price")
            print(e)
        self.conn.commit()


    def insert_Hfacility(self,item):
        if item["Hfacility"]:
            sql = "INSERT INTO HotelFacility (FID,HId,CId,FirstName,SecondName) VALUES "
            for Hfacility in item["Hfacility"]:
                first_name = list(Hfacility.keys())[0]
                seconds = Hfacility[first_name]
                for second in seconds:
                    str_data = hashlib.md5((str(item["HId"])+str(first_name)+str(second)).encode("utf-8")).hexdigest()
                    sql = sql + "('%s','%s','%s','%s','%s')" %(str_data,item["HId"],item["City"],first_name,second)

            sql = sql.replace(')(','),(')

            try:
                self.cur.execute(sql)
            except Exception as e:
                pass
            self.conn.commit()

    def insert_Ofacility(self,item):
        if item["Ofacility"]:
            sql = "INSERT INTO OtherFacility (OId,HId,CId,FirstName,SecondName) VALUES "
            for Ofacility in item["Ofacility"]:
                first_name = list(Ofacility.keys())[0]
                seconds = Ofacility[first_name]
                for second in seconds:
                    str_data = hashlib.md5((str(item["HId"])+str(first_name)+str(second)).encode("utf-8")).hexdigest()
                    sql = sql + "('%s','%s','%s','%s','%s')" % (str_data, item["HId"],item["City"],first_name, second)
            sql = sql.replace(')(', '),(')

            try:
                self.cur.execute(sql)
            except Exception as e:
                pass
            self.conn.commit()

    def insert_Allimage(self,item):
        sql = "insert into AllImage (PId,HId,Title,Url) values "
        for pid in item["pic_id"]:
            ind = item["pic_id"].index(pid)
            title = item["pic_title"][ind]
            url = item["pic_url"][ind]
            sql = sql + "('%s','%s','%s','%s')" %(str(pid),str(item["HId"]),str(title),str(url))
        sql = sql.replace(')(', '),(')

        try:
            self.cur.execute(sql)
        except Exception as e:
            # print(e)
            pass
        self.conn.commit()