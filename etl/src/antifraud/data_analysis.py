# -*- coding: utf-8 -*-
import json

import pandas
import pymysql

from src.util.arrays import chunks
from src.util.database import load_config, connect_db

db_config = {}
app_mp_map = {}
result = {}
root_path = "/home/fred/Documents/2.rmd/1.antifraud"


def load_app_csv():
    tpot_data = pandas.read_csv(root_path + '/医美欺诈用户数据.csv')
    # features = tpot_data.drop('target', axis=1).values

    return tpot_data.values


def get_app_data(app_list):
    for app_sub_list in chunks(app_list, 1000):
        app_id_list = []
        for app in app_sub_list:
            app_id_list.append(app[1])
        id_list_str = "'" + "','".join(app_id_list) + "'"

        # 基础信息
        sql_str = "SELECT * FROM BIZ_APP_COMMON where C_APP_ID in (%s)" % id_list_str
        con = connect_db(db_config['datacenter'], 'datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql_str)
        rows = cur.fetchall()
        cur.close()
        con.close()

        for row in rows:
            app_mp_map[row['C_APP_ID']] = row['C_MBL_TEL']


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

            sql_str = "select 1 from %s where %s in (%s)" % (
                dict_item['table'], dict_item['search_field'], search_val)

            con = connect_db(db_config['datacenter'], 'datacenter')
            cur = con.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute(sql_str)
            res = cur.fetchall()
            cur.close()
            con.close()
            print(dict_item['name'], ' length: ', len(res))
            if result.__contains__(dict_item['name']):
                result[dict_item['name']]['num'] = result[dict_item['name']]['num'] + len(res)
            else:
                result[dict_item['name']] = {}
                result[dict_item['name']]['num'] = len(res)


def analysis(total_num):
    for key in result.keys():
        result[key]['per'] = "%.2f" % ((result[key]['num'] / total_num) * 100)

    result['total_num'] = total_num


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()
    # 获取订单结果字典
    app_definition = load_app_csv()

    get_app_data(app_definition)
    get_3rd_data(app_definition)

    analysis(len(app_definition))

    print(result)
