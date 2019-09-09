# -*- coding: utf-8 -*-

# 凭安
import os

import pymysql

from src.util.database import connect_db


def fetch_batch(app_list_str, db_config):
    sql_str = "select RAW_DATA, APP_ID, ID from T_PA_REQ_DATA where APP_ID in (%s) " % app_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res


def fetch_batch_og(app_list_str, db_config):
    sql_str = "select C_RES_JSON, C_APP_ID, ID from GEEX_PA_APPLY_RES_RECORD where C_APP_ID in (%s) " % app_list_str
    print('old', sql_str)

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    print('old finish')
    cur.close()
    con.close()

    return res


def fetch_batch_and_store_2018(db_config, path):
    sql_str = "select C_RES_JSON, C_APP_ID, ID from GEEX_PA_APPLY_RES_RECORD where CREATE_DATE between '2018-01-01' and '2019-01-01'"
    print('get pa 2018', sql_str)

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    for line in res:
        appid = line['C_APP_ID']
        raw_data = line['C_RES_JSON']
        id = line['ID']

        if raw_data != None and os.path.exists(path + '/' + appid):
            file = open(path + '/' + appid + '/pa_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()


def fetch_batch_and_store(app_list_str, db_config, path):
    print('get pa', app_list_str)
    res = fetch_batch(app_list_str, db_config)
    for line in res:
        appid = line['APP_ID']
        raw_data = line['RAW_DATA']
        id = line['ID']

        if raw_data != None:
            file = open(path + '/' + appid + '/pa_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()
