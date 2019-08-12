# -*- coding: utf-8 -*-
import csv

from sqlalchemy.dialects.mysql import pymysql

from src.database import *


def load_app_csv():
    res = {}

    with open('/home/fred/Documents/rmd/antifraud/医美欺诈用户数据5000.csv') as f:
        f_csv = csv.DictReader(f)
        for row in f_csv:
            res[row['C_APP_ID']] = row['Y']

    return res


def chunks(arr, n):
    return [arr[i:i + n] for i in range(0, len(arr), n)]


def get_data(app_id_list):
    app_main = {}

    for id_list in chunks(app_id_list, 5000):
        id_list_str = "'" + "','".join(id_list) + "'"

        # 基础信息
        sql_str = "SELECT * FROM BIZ_APP_COMMON where C_APP_ID in (%s)" % (id_list_str)
        con = connect_db('datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql_str)
        rows = cur.fetchall()
        cur.close()
        con.close()
        for row in rows:
            if not app_main.__contains__(row['C_APP_ID']):
                app_main[row['C_APP_ID']] = {}
            app_main[row['C_APP_ID']]['common'] = row

        # 01信息
        sql_str = "SELECT * FROM BIZ_APP01 where C_APP_ID in (%s)" % (id_list_str)
        con = connect_db('datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(sql_str)
        rows = cur.fetchall()
        cur.close()
        con.close()
        for row in rows:
            app_main[row['C_APP_ID']]['01'] = row

    return app_main


def save_data(app_data, app_definition):
    # 制作header
    header = []
    for key in app_data:
        common_keys = list(app_data[key]['common'].keys())
        ext_keys = list(app_data[key]['01'].keys())
        del ext_keys[0]
        del ext_keys[0]
        header = common_keys + ext_keys
        break
    result_header = ['result']

    # 封装数据
    rows = []
    result_rows = []
    for key in app_data:
        # 参数
        row = {**app_data[key]['common'], **app_data[key]['01']}
        rows.append(row)

        # 结果数据
        result_row = {'result': app_definition[key]}
        result_rows.append(result_row)

    print(len(rows))
    print(len(result_rows))

    # 参数数据
    with open('/home/fred/Documents/rmd/antifraud/out/app.csv', 'w') as f:
        f_csv = csv.DictWriter(f, header)
        f_csv.writeheader()
        f_csv.writerows(rows)

    # 结果数据
    with open('/home/fred/Documents/rmd/antifraud/out/app_result.csv', 'w') as f:
        f_csv = csv.DictWriter(f, result_header)
        f_csv.writeheader()
        f_csv.writerows(result_rows)


if __name__ == '__main__':
    # 获取订单结果字典
    app_definition = load_app_csv()

    # 拼接订单数组
    app_list = []
    for key in app_definition:
        app_list.append(key)

    # 获取订单数据
    app_data = get_data(app_list)

    # 保存数据
    save_data(app_data, app_definition)
