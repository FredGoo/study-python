# -*- coding: utf-8 -*-

# 集奥
import pymysql

from src.util.database import connect_db


def fetch_batch(app_list_str, db_config):
    sql_str = "select RAW_DATA, APP_ID,ID from T_GEO_REQ_DATA where APP_ID in (%s) " % app_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res


def fetch_batch_og(mbl_list_str, db_config):
    sql_str = "select DATA, PHONENUM,ID from T_GEO where PHONENUM in (%s) " % mbl_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res


def fetch_batch_and_store(mbl_str, app_list_str, mbl_app_map, db_config, path):
    print('get ja', mbl_str, app_list_str)
    res = fetch_batch(app_list_str, db_config)
    for line in res:
        appid = line['APP_ID']
        raw_data = line['RAW_DATA']
        id = line['ID']

        if raw_data != None:
            file = open(path + '/' + appid + '/ja_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()

    res = fetch_batch_og(mbl_str, db_config)
    for line in res:
        mbl = line['PHONENUM']
        appid = mbl_app_map[mbl]
        raw_data = line['DATA']
        id = line['ID']

        if raw_data != None:
            file = open(path + '/' + appid + '/ja_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()
