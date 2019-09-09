# -*- coding: utf-8 -*-

# 天行
import pymysql

from src.util.database import connect_db


def fetch_batch(app_list_str, db_config):
    sql_str = "select RAW_DATA, APP_ID, ID from T_TX_REQ_DATA where APP_ID in (%s) " % app_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res


def fetch_batch_og(app_list_str, db_config):
    sql_str = "select C_RES_JSON, C_APP_ID, ID from GEEX_TX_RES_RECORD where C_APP_ID in (%s) " % app_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res


def fetch_batch_and_store(app_list_str, db_config, path):
    print('get tx', app_list_str)
    res = fetch_batch(app_list_str, db_config)
    for line in res:
        appid = line['APP_ID']
        raw_data = line['RAW_DATA']
        id = line['ID']

        if raw_data != None:
            file = open(path + '/' + appid + '/tx_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()

    res = fetch_batch_og(app_list_str, db_config)
    for line in res:
        appid = line['C_APP_ID']
        raw_data = line['C_RES_JSON']
        id = line['ID']

        if raw_data != None:
            file = open(path + '/' + appid + '/tx_' + str(id) + '.json', 'w')
            file.write(raw_data)
            file.close()
