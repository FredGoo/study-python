# -*- coding: utf-8 -*-
import json
import os

import pymysql

from src.util.arrays import chunks
from src.util.database import load_config, connect_db
from src.util.jsonlib import CustomEncoder

db_config = {}
output_path = '/home/fred/data/sample20191108/raw/'


# 获取订单列表
def get_app_list():
    sql = "select app_id from kezhi.app_id_list where deal_status = 0"

    con = connect_db(db_config['local'], 'kezhi')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


def start(app_list):
    appbatchsize = 1000
    i = 1
    for app_sub_list in chunks(app_list, appbatchsize):
        print('appnum', i * appbatchsize)
        i += 1

        app_id_list = []
        for app in app_sub_list:
            app_id_list.append(app['app_id'])

        # 拼接查询条件
        app_id_str = "'" + "','".join(app_id_list) + "'"

        # 百融征信打包
        get_zxdb_new(app_id_str)
        get_zxdb_old(app_id_str)

        # 百融消费评分
        get_xfpf(app_id_str)


def get_xfpf(appidlist):
    sql = 'SELECT distinct APP_ID, T.* FROM T_BR_REQ_DATA T WHERE T.APP_ID in (%s) and T.API_CODE = "BRXFPF" and RES_CODE = 00' % appidlist
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    sqlres = cur.fetchall()

    cur.close()
    con.close()

    if sqlres != None:
        for sqlresitem in sqlres:
            sqlresitemres = json.loads(sqlresitem['RAW_DATA'])

            appid = sqlresitem['APP_ID']
            # 数据落地
            folder_path = os.path.join(output_path, appid)
            folder_exists = os.path.exists(folder_path)
            if not folder_exists:
                os.makedirs(folder_path)

            with open(os.path.join(output_path, appid, 'br_BRXFPF.json'), 'w') as f:
                json.dump(sqlresitemres, f, cls=CustomEncoder)


def get_zxdb_new(appidlist):
    res = {}

    # 获取新数据
    new_br_main_sql = 'select distinct APP_ID, ID from T_BR_REQ_DATA where APP_ID in (%s) and API_CODE = "BRZXDB" and RES_CODE = 00 order by ID' % appidlist
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(new_br_main_sql)
    newbrmain = cur.fetchall()
    cur.close()
    con.close()

    if newbrmain != None and len(newbrmain) > 0:
        relationidlist = []
        for newbrmainkey in newbrmain:
            relationidlist.append(str(newbrmainkey['ID']))
        relation_id_str = ",".join(relationidlist)

        new_br_zxdb_sql = 'select APP_ID, HASH_KEY, HASH_VALUE from T_BRZXDB_DETAIL_DATA where RELATION_ID in (%s)' % relation_id_str
        con = connect_db(db_config['datacenter'], 'datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(new_br_zxdb_sql)
        newbrzxdb = cur.fetchall()
        cur.close()
        con.close()

        for newbrzxdbitem in newbrzxdb:
            if not res.__contains__(newbrzxdbitem['APP_ID']):
                res[newbrzxdbitem['APP_ID']] = {}

            res[newbrzxdbitem['APP_ID']][newbrzxdbitem['HASH_KEY']] = newbrzxdbitem['HASH_VALUE']

    # 数据落地
    for appid in res.keys():
        folder_path = os.path.join(output_path, appid)
        folder_exists = os.path.exists(folder_path)
        if not folder_exists:
            os.makedirs(folder_path)

        with open(os.path.join(output_path, appid, 'br_BRZXDB.json'), 'w') as f:
            json.dump(res[appid], f, cls=CustomEncoder)


def get_zxdb_old(appidlist):
    old_br_apply_sql = 'select * from GEEX_BS_APPLY_LOAN_STR str join GEEX_BS_SPECIAL_LIST lit on str.APP_ID = lit.APP_ID join GEEX_BS_INFO_RELATION rel on rel.APP_ID = str.APP_ID where str.APP_ID in (%s) and (str.CODE = 00 or lit.CODE = 00 or rel.CODE = 00)' % appidlist

    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(old_br_apply_sql)
    oldbrapply = cur.fetchall()
    cur.close()
    con.close()

    if oldbrapply != None:
        for oldbrapplyitem in oldbrapply:
            oldbrapplyres = {}
            for key in oldbrapplyitem.keys():
                oldbrapplyres[key.lower()] = oldbrapplyitem[key]

            appid = oldbrapplyres['app_id']
            folder_path = os.path.join(output_path, appid)
            folder_exists = os.path.exists(folder_path)
            if not folder_exists:
                os.makedirs(folder_path)

            with open(os.path.join(output_path, appid, 'br_BRZXDB.json'), 'w') as f:
                json.dump(oldbrapplyres, f, cls=CustomEncoder)


if __name__ == '__main__':
    db_config = load_config()

    app_list = get_app_list()
    start(app_list)

    # 测试
    # app_id_list = ['NYB01-191108-238531', 'NYB01-191108-176517', 'NYB01-191108-539184', 'NYB01-180801-545342']
    # app_id_str = "'" + "','".join(app_id_list) + "'"
    # get_zxdb_new(app_id_str)
    # get_zxdb_old(app_id_str)
    # get_xfpf(app_id_str)
