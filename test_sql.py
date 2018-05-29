import pymssql


class Test(object):
    def __init__(self):
        self.conn = pymssql.connect(host='192.168.2.135\sql2008', user='sa', password='sa', database='HotelSpider')
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    # 测试查询酒店
    def select_hotel(self,HId):
        select = "select count(*) from Hotel where HId='%s'" % str(HId)
        ret = None
        try:
            self.cur.execute(select)
            ret = self.cur.fetchone()
            print("查询成功hotel")
        except Exception as e:
            print("查询失败hotel")
            print(e)
        # print(ret[0])
        # print(type(ret[0]))
        if ret[0]:
            print("if ret")
        else:
            print("not ret")
        self.conn.commit()
        return ret

    # 更新酒店测试
    def update_hotel(self,HId):
        update = "update Hotel set Score='%f',Price='%.2f',Phone='%s',RoomCount='%s' where HId='%s'" %(
            float(4.5),float(1293),str("010-6618668"),str(555),str(HId)
        )
        try:
            self.cur.execute(update)
            print("更新成功")
            self.conn.commit()
        except Exception as e:
            print("更新失败")
            print(e)
            self.conn.rollback()

if __name__ == '__main__':
    pass
    # test = Test()
    # # test.select_hotel(939388111)
    # test.update_hotel(939388)
