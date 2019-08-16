# -*- coding: utf-8 -*-
import json

import pandas
import pymysql

from src.util.arrays import chunks
from src.util.database import connect_db, load_config
from src.util.file import save_batch_json, save_json

db_config = {}
app_mp_map = {}
root_path = "/home/fred/Documents/2.rmd/1.antifraud"
output_path = root_path + "/out/data20190816"


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

        save_batch_json(rows, 'C_APP_ID', output_path + '/raw', 'appinfo')


def get_3rd_data(app_list):
    # 获取三方数据配置
    with open('../config/3rd_party.json', 'r') as f:
        datasoucre_dict = json.load(fp=f)

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

            con = connect_db(db_config['datacenter'], 'datacenter')
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
                        subfolder = mp_app_map[subfolder]

                    raw_data = res_item[dict_item['data_field']]
                    # 写入数据
                    if raw_data != None:
                        save_json(raw_data, subfolder, output_path + '/raw',
                                  dict_item['name'] + '_' + str(res_item[dict_item['data_id']]))


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
