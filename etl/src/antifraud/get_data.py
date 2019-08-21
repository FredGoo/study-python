# -*- coding: utf-8 -*-
import json

import pandas
import pymysql

from src.antifraud.thirdparty.mx import fetch_batch
from src.util.arrays import chunks
from src.util.database import connect_db, load_config
from src.util.file import save_json

db_config = {}
app_mp_map = {}
root_path = "/home/fred/Documents/2.rmd/1.antifraud"
output_path = root_path + "/out/data20190821"


def load_app_csv():
    tpot_data = pandas.read_csv(root_path + '/医美欺诈用户数据.csv')
    # features = tpot_data.drop('target', axis=1).values

    return tpot_data.values


def save_app_target(app_list):
    for app in app_list:
        app_item = {
            'C_APP_ID': app[1],
            'C_CUST_IDNO': app[2],
            'N_TARGET': app[3]
        }
        save_json(app_item, app[1], output_path + '/raw', 'app_target')


def get_app_data(app_list):
    for app_sub_list in chunks(app_list, 1000):
        app_id_list = []
        for app in app_sub_list:
            app_id_list.append(app[1])
        id_list_str = "'" + "','".join(app_id_list) + "'"

        # 基础信息
        sql_str = "SELECT * FROM BIZ_APP_COMMON common left join BIZ_APP01 ext on ext.C_APP_ID = common.C_APP_ID  where common.C_APP_ID in (%s)" % id_list_str
        con = connect_db(db_config['datacenter'], 'datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql_str)
        rows = cur.fetchall()
        cur.close()
        con.close()

        for row in rows:
            app_mp_map[row['C_APP_ID']] = row['C_MBL_TEL']

        # save_batch_json(rows, 'C_APP_ID', output_path + '/raw', 'appinfo')


def get_3rd_data(app_list):
    mx_num = 0
    for app_sub_list in chunks(app_list, 1000):
        app_id_list = []
        mp_list = []
        mp_app_map = {}
        for app in app_sub_list:
            appid = app[1]
            mp = app_mp_map[app[1]]

            app_id_list.append(appid)
            mp_list.append(mp)
            mp_app_map[mp] = appid

        # 拼接查询条件
        app_id_str = "'" + "','".join(app_id_list) + "'"
        # 拼接手机号查询条件
        mbl_str = "'" + "','".join(mp_list) + "'"

        # 魔蝎处理
        res = fetch_batch(mbl_str, db_config['datacenter'])
        print('mx res length: ', len(res))
        mx_num = mx_num + len(res)
        save_3rd_file(res, mp_app_map, 'mbl', 'mx')
    print('mx total num', mx_num)


def save_3rd_file(data, mp_app_map, datatype, name):
    if (len(data) > 0):
        for data_item in data:
            subfolder = data_item['ckey']
            if datatype == 'mbl':
                subfolder = mp_app_map[subfolder]

            raw_data = data_item['cdata']
            if raw_data == '':
                continue

            if type(raw_data) == str:
                save_data = json.loads(raw_data)
            else:
                save_data = raw_data

            # 写入数据
            save_json(save_data, subfolder, output_path + '/raw',
                      name + '_' + str(data_item['cid']))


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()

    # 获取订单结果字典
    app_definition = load_app_csv()

    # 保存订单标签
    save_app_target(load_app_csv())

    # 获取订单数据
    get_app_data(app_definition)

    # 获取三方数据
    get_3rd_data(app_definition)
