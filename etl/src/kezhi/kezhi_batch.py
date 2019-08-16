# -*- coding: utf-8 -*-
import datetime
import decimal
import json
import os

import pymysql

from src.util.database import connect_db, load_config

# 输出目录
output_folder = '/home/fred/Documents/2.rmd/2.kezhi/sample20190815'
# 执行批次号
batch_no = 5

# datacenter
datacenter_config = {}
# 127.0.0.1
local_config = {}


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        return super(CustomEncoder, self).default(obj)


def chunks(arr, n):
    return [arr[i:i + n] for i in range(0, len(arr), n)]


# 获取订单信息
def get_app_info(app_list):
    for app_sub_list in chunks(app_list, 1000):
        app_id_list = []
        for app in app_sub_list:
            app_id_list.append(app['app_id'])
        # 拼接查询条件
        app_id_str = "'" + "','".join(app_id_list) + "'"

        # 获取订单信息
        sql_str = "select * from BIZ_APP_COMMON common left join BIZ_APP01 ext on common.C_APP_ID = ext.C_APP_ID where common.C_APP_ID in (%s)" % app_id_str
        con = connect_db(datacenter_config, 'datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql_str)
        res = cur.fetchall()
        cur.close()
        con.close()
        print('app info res length: ', len(res))

        if (len(res) > 0):
            for res_item in res:
                subfolder = res_item['C_APP_ID']

                print('app info: ', res_item)
                # 写入三方数据
                path = output_folder + '/raw/' + subfolder

                # 创建文件夹
                folder_exists = os.path.exists(path)
                if not folder_exists:
                    os.makedirs(path)

                with open(path + '/appinfo.json', 'w') as f:
                    json.dump(res_item, f, cls=CustomEncoder, ensure_ascii=False)


# 查询三方数据
def get_3rd_data(app_list):
    # 获取三方数据配置
    with open('../config/3rd_party.json', 'r') as f:
        datasoucre_dict = json.load(fp=f)

    for app_sub_list in chunks(app_list, 1000):
        app_id_list = []
        mbl_list = []
        mbl_app_map = {}
        for app in app_sub_list:
            app_id_list.append(app['app_id'])
            mbl_list.append(app['custom_mobile'])
            mbl_app_map[app['custom_mobile']] = app['app_id']

        # 拼接查询条件
        app_id_str = "'" + "','".join(app_id_list) + "'"
        # 拼接手机号查询条件
        mbl_str = "'" + "','".join(mbl_list) + "'"

        # 查询第三方数据
        for dict_item in datasoucre_dict:
            search_val = ""
            if dict_item['info_field'] == 'C_APP_ID':
                search_val = app_id_str
            if dict_item['info_field'] == 'C_MBL_TEL':
                search_val = mbl_str

            sql_str = "select * from %s where %s in (%s)" % (
                dict_item['table'], dict_item['search_field'], search_val)
            print('3rd sql: ', sql_str)

            con = connect_db(datacenter_config, 'datacenter')
            cur = con.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute(sql_str)
            res = cur.fetchall()
            cur.close()
            con.close()
            print('3rd res length: ', len(res))

            if (len(res) > 0):
                for res_item in res:
                    subfolder = res_item[dict_item['search_field']]
                    if dict_item['info_field'] == 'C_MBL_TEL':
                        subfolder = mbl_app_map[subfolder]

                    raw_data = res_item[dict_item['data_field']]
                    print('3rd res raw data: ', raw_data)
                    # 写入三方数据
                    path = output_folder + '/raw/' + subfolder

                    # 创建文件夹
                    folder_exists = os.path.exists(path)
                    if not folder_exists:
                        os.makedirs(path)

                    if raw_data != None:
                        file = open(
                            path + '/' + dict_item['name'] + '_' + str(res_item[dict_item['data_id']]) + '.json',
                            'w')
                        file.write(raw_data)
                        file.close()


# 获取订单列表
def get_app_list(batch_id):
    sql = "select * from kezhi.app_id_list where batch_id = %s and deal_status = 0" % batch_id

    con = connect_db(local_config, 'kezhi')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()
    datacenter_config = db_config['datacenter']
    local_config = db_config['local']

    # 获取订单列表
    app_list = get_app_list(batch_no)

    # 获取三方数据
    get_3rd_data(app_list)

    # 获取订单信息
    get_app_info(app_list)
