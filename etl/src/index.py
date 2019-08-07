# encoding:utf-8

import datetime
import decimal
import json
import os

import pymysql


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        return super(CustomEncoder, self).default(obj)


def connect_db(database):
    with open('json/env.json', 'r') as f:
        env = json.load(fp=f)

    return pymysql.connect(host=env['host'],
                           port=env['port'],
                           user=env['user'],
                           password=env['password'],
                           database=database,
                           charset='utf8')


def query_biz_app(appid):
    sql_str = "SELECT * FROM gravity.BIZ_APP_COMMON where C_APP_ID = '%s'" % (appid)

    con = connect_db('gravity')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


def query_3rd_data(table, field, field_value):
    sql_str = "select * from xrisk.%s where %s = '%s'" % (table, field, field_value)
    print('3rd sql: ', sql_str)

    con = connect_db('gravity')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


def create_app_folder(appid):
    path = os.getcwd()[:-4] + '/out/' + appid

    # 创建文件夹
    folder_exists = os.path.exists(path)
    if not folder_exists:
        os.makedirs(path)

    return path


def save_app_info(appinfo, path):
    # name = appinfo['C_NAME_CN']
    # idno = appinfo['C_CUST_IDNO']
    # mp = appinfo['C_MBL_TEL']

    # 写入基础数据
    with open(path + '/appinfo.json', 'w') as f:
        json.dump(appinfo, f, cls=CustomEncoder, ensure_ascii=False)


def save_3rd_data(appinfo, path, test=False):
    # 获取三方数据配置
    with open('json/data.json', 'r') as f:
        datasoucre_dict = json.load(fp=f)

    # 查询第三方数据
    for dict_item in datasoucre_dict:
        save_3rd_data_item(appinfo, path, dict_item, test)


def save_3rd_data_item(appinfo, path, dict_item, test):
    if test:
        field_value = dict_item['test_value']
    else:
        field_value = appinfo[dict_item['info_field']]

    res = query_3rd_data(dict_item['table'], dict_item['search_field'], field_value)
    print('3rd res length: ', len(res))

    if (len(res) > 0):
        for res_item in res:
            raw_data = res_item[dict_item['data_field']]
            print('3rd res raw data: ', raw_data)
            # 写入三方数据
            file = open(path + '/' + dict_item['name'] + '_' + str(res_item[dict_item['data_id']]) + '.json', 'w')
            file.write(raw_data)
            file.close()


if __name__ == '__main__':
    # 需要查询的订单
    appid = 'NYB01-181204-200610'

    # 获取订单基础数据
    appinfo = query_biz_app(appid)[0]

    # 创建数据目录
    path = create_app_folder(appid)
    # 保存主订单信息
    save_app_info(appinfo, path)
    # 保存三方数据
    save_3rd_data(appinfo, path, test=True)
