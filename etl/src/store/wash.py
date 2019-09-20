# -*- coding: utf-8 -*-
import csv
import datetime
import decimal
import json
import os
import re

import pymysql

from src.thirdparty import mx
from src.util.database import connect_db, load_config
from src.util.phone import oddphone_filter

root_path = '/home/fred/Documents/2.rmd/tmp/data20190920'
db_config = {}


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        return super(CustomEncoder, self).default(obj)


mx_map = {}


# 图信息清洗
def graph_wash(path):
    ran_dir_num = 0

    mobile_dict = {}
    mobile_contact_list = []
    mobile_contract_list = []
    store_dict = {}
    store_list = []

    # 遍历文件夹
    dir_list = os.listdir(path)
    total_app_num = len(dir_list)
    print('total app', len(dir_list))
    for dir in dir_list:
        dir_path = path + '/' + dir
        if os.path.isdir(dir_path):
            ran_dir_num += 1
            print('appid', dir)
            print('ran app num', str(ran_dir_num) + '/' + str(total_app_num))

            # 处理每个订单的数据
            appinfo_file_path = None
            mx_file_path = None
            old_mx = 0
            for file in os.listdir(dir_path):
                if file.startswith('appinfo'):
                    appinfo_file_path = dir_path + '/' + file
                elif file.startswith('mx'):
                    if mx_file_path == None:
                        mx_file_path = dir_path + '/' + file
                        old_mx = file.split('.')[0].split('_')[1]
                    else:
                        new_mx = file.split('.')[0].split('_')[1]
                        if new_mx > old_mx:
                            mx_file_path = dir_path + '/' + file

            # 获取订单和魔蝎数据
            with open(appinfo_file_path) as f:
                appinfo = json.load(fp=f)
            if mx_file_path != None:
                mxinfo = mx.wash_contact_list(mx_file_path)
            else:
                mxinfo = None

            # 生成可导入neo4j的数据
            user_phone = appinfo['C_MBL_TEL']
            if not mobile_dict.__contains__(user_phone):
                mobile_dict[user_phone] = {
                    'mobile:ID': user_phone,
                    'idno': appinfo['C_CUST_IDNO'],
                    'name': appinfo['C_NAME_CN'],
                    'app_id': appinfo['C_APP_ID'],
                    'overdue_days': appinfo['OVERDUE_DAYS'],
                    'app_status': appinfo['N_APP_STATUS'],
                    'store': appinfo['C_ORG04']
                }

                if not store_dict.__contains__(appinfo['C_ORG04']):
                    store_dict[appinfo['C_ORG04']] = {'store:ID': appinfo['C_ORG04']}
                store_list.append({
                    ":START_ID": user_phone,
                    ":END_ID": appinfo['C_ORG04'],
                    ':TYPE': 'STORE_AT'
                })
            if mxinfo != None:
                for contact in mxinfo['data']:
                    if not oddphone_filter(contact['phone_num']):
                        # 通讯关系
                        if not mobile_dict.__contains__(contact['phone_num']):
                            contact_user = {'mobile:ID': contact['phone_num']}
                            mobile_dict[str(contact['phone_num'])] = contact_user
                        if contact['call_in_cnt'] > 0:
                            mobile_contact_list.append({
                                ":END_ID": user_phone,
                                ":START_ID": contact['phone_num'],
                                ':TYPE': 'CALLS'
                            })
                        if contact['call_out_cnt'] > 0:
                            mobile_contact_list.append({
                                ":END_ID": contact['phone_num'],
                                ":START_ID": user_phone,
                                ':TYPE': 'CALLS'
                            })

            print('------')

        # 测试用语句
        # if ran_dir_num > 9:
        #     break

    # 处理通讯录信息
    with open(root_path + '/contract201807to201812.csv') as f:
        fcsv = csv.DictReader(f)
        run_contact_num = 1
        for line in fcsv:
            if mobile_dict.__contains__(line['USER_MOBILE']):
                if not mobile_dict.__contains__(line['MOBILE']):
                    contact_user = {
                        'mobile:ID': line['MOBILE'],
                        'name': line['NAME']
                    }
                    mobile_dict[str(line['MOBILE'])] = contact_user
                else:
                    mobile_dict[str(line['MOBILE'])]['name'] = line['NAME']

                mobile_contract_list.append({
                    ":START_ID": line['USER_MOBILE'],
                    ":END_ID": line['MOBILE'],
                    ':TYPE': 'CONTACTS'
                })

                run_contact_num += 1
                print('ran contact num', run_contact_num)

            # 测试用语句
            # if run_contact_num > 9:
            #     break

    export_path = root_path + '/csv'
    with open(export_path + '/mobile.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['mobile:ID', 'app_id', 'idno', 'overdue_days', 'app_status', 'store', 'name'])
        f_csv.writeheader()
        f_csv.writerows(mobile_dict.values())
    with open(export_path + '/mobile_call_relation.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(mobile_contact_list)
    with open(export_path + '/mobile_contact_relation.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(mobile_contract_list)

    with open(export_path + '/store.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['store:ID'])
        f_csv.writeheader()
        f_csv.writerows(store_dict.values())
    with open(export_path + '/mobile_store_relation.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(store_list)


def instinct_contact():
    export_path = root_path + '/csv'

    # 魔蝎箱单
    i = 0
    res = {}
    with open(export_path + '/mobile_call_relation.csv', 'r') as f:
        fcsv = csv.DictReader(f)
        for line in fcsv:
            print(i)
            i += 1

            if not res.__contains__(line[':START_ID'] + line[':END_ID']):
                res[line[':START_ID'] + line[':END_ID']] = line

    with open(export_path + '/mobile_call_relation2.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(res.values())

    # 通讯录
    i = 0
    res = {}
    with open(export_path + '/mobile_contact_relation.csv', 'r') as f:
        fcsv = csv.DictReader(f)
        for line in fcsv:
            print(i)
            i += 1

            if not res.__contains__(line[':START_ID'] + line[':END_ID']):
                res[line[':START_ID'] + line[':END_ID']] = line

    with open(export_path + '/mobile_contact_relation2.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(res.values())


def store_info_wash():
    res = []

    con = connect_db(db_config['datacenter'], 'datacenter')
    with open(root_path + '/csv/store.csv', 'r', newline='')as f:
        f_csv = csv.DictReader(f)
        for line in f_csv:
            store_code = line['store:ID']

            store_info_sql = '''
                select C_NAME, C_STORE_CODE, C_LEVEL from BIZ_STORE_ORGNZ where C_STORE_CODE = '%s';
            '''
            cur = con.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute(store_info_sql % store_code)
            store_info = cur.fetchone()
            cur.close()

            store_all_app_sql = '''
                select count(1) as num from T_LOAN where STORE_CODE = '%s';
            '''
            cur = con.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute(store_all_app_sql % store_code)
            store_all_info = cur.fetchone()
            cur.close()

            store_overdue_sql = '''
                select count(1) as num from T_LOAN where STORE_CODE = '%s' and OVERDUE_DAYS > 0;
            '''
            cur = con.cursor(cursor=pymysql.cursors.DictCursor)
            cur.execute(store_overdue_sql % store_code)
            store_overdue_info = cur.fetchone()
            cur.close()

            store = {
                'store:ID': store_code,
                'name': store_info['C_NAME'],
                'level': store_info['C_LEVEL']
            }
            if store_all_info['num'] > 0:
                store['overdue_rate'] = store_overdue_info['num'] / store_all_info['num']
            res.append(store)
            print(store)

    con.close()

    with open(root_path + '/csv/store2.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['store:ID', 'name', 'level', 'overdue_rate'])
        f_csv.writeheader()
        f_csv.writerows(res)


def mobile_csv_wash():
    res = []

    with open(root_path + '/csv/mobile.csv', 'r', newline='')as f:
        pattern = re.compile(r'[^\u4e00-\u9fa5]')

        f_csv = csv.DictReader(f)
        for line in f_csv:
            if line['name'] != '':
                line['name'] = re.sub(pattern, '', line['name'])
            line['mobile:ID'] = re.sub("\D", "", line['mobile:ID'])

            res.append(line)

    with open(root_path + '/csv/mobile2.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['mobile:ID', 'app_id', 'idno', 'overdue_days', 'app_status', 'store', 'name'])
        f_csv.writeheader()
        f_csv.writerows(res)


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()
    graph_wash(root_path + '/raw')
    instinct_contact()
    store_info_wash()
    mobile_csv_wash()
