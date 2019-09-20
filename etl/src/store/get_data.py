# -*- coding: utf-8 -*-
import json
import math

import pymysql

from src.thirdparty.mx import fetch_batch
from src.util.arrays import chunks
from src.util.database import connect_db, load_config
from src.util.file import save_json, save_batch_json

db_config = {}
app_mp_map = {}
root_path = "/home/fred/Documents/2.rmd/tmp"
output_path = root_path + "/data20190920"


def get_app_data_range_date():
    # 获取订单总数
    count_app_sql = '''
        select count(1) as total_num
        from BIZ_APP_COMMON b
                 left join T_LOAN t on b.C_APP_ID = t.EXT_ID
        where b.D_CREATE between '2018-07-01' and '2019-01-01'
          and b.N_APP_STATUS in (160, 162)
          and b.C_APP_TYPE not in ('GMAIN', 'GUP', 'MAIN')
    '''
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(count_app_sql)
    app_count = cur.fetchone()
    cur.close()
    con.close()
    total_num = app_count['total_num']

    # 分页获取订单数量
    each_page_num = 5000
    page = math.ceil(total_num / each_page_num)
    page_now = 1
    page_sql = '''
        select *
        from BIZ_APP_COMMON b
                 left join T_LOAN t on b.C_APP_ID = t.EXT_ID
        where b.D_CREATE between '2018-07-01' and '2019-01-01'
          and b.N_APP_STATUS in (160, 162)
          and b.C_APP_TYPE not in ('GMAIN', 'GUP', 'MAIN')
          order by N_APP_ID
          limit %s, %s
    '''
    while page_now <= page:
        con = connect_db(db_config['datacenter'], 'datacenter')
        cur = con.cursor(cursor=pymysql.cursors.DictCursor)
        cur.execute(page_sql % ((page_now - 1) * each_page_num, each_page_num))
        app_batch = cur.fetchall()
        cur.close()
        con.close()
        save_batch_json(app_batch, 'C_APP_ID', output_path + '/raw', 'appinfo')
        get_3rd_data(app_batch)
        # break
        page_now += 1


def get_3rd_data(app_list):
    mx_num = 0
    for app_sub_list in chunks(app_list, 1000):
        mp_list = []
        mp_app_map = {}
        for app in app_sub_list:
            mp_list.append(app['C_MBL_TEL'])
            mp_app_map[app['C_MBL_TEL']] = app['C_APP_ID']

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

    # 获取订单信息
    get_app_data_range_date()
