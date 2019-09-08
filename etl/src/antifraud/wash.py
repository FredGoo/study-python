# -*- coding: utf-8 -*-
import csv
import datetime
import decimal
import json
import os

from src.antifraud.thirdparty import mx
from src.util.phone import oddphone_filter

root_path = '/home/fred/Documents/neo4j/data20190903'


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


# 加载魔蝎配置
def load_mx_map():
    with open('../config/3rd/mx.json', 'r') as f:
        return json.load(fp=f)


def mx_key_map():
    with open('../config/3rd/mx.json', 'r') as f:
        mxjson = json.load(fp=f)

        for key in mxjson['application_check']:
            mxjson['application_check'][key] = 'application_check_' + mxjson['application_check'][key]

        print(mxjson)


def norm_wash(path):
    i = 0
    j = 0

    for root, dirs, files in os.walk(path):
        i = i + 1
        for file in files:
            if file.startswith('mx'):
                j = j + 1
                print('folder', i, 'file', j)

                res = mx.wash(root + '/' + file, mx_map)
                if res == 0:
                    return
                with open(root + '/first_raw_without_array_mx.json', 'w') as f:
                    json.dump(res, f, cls=CustomEncoder, ensure_ascii=False)


# 图信息清洗
def graph_wash(path):
    ran_dir_num = 0

    mobile_dict = {}
    mobile_contact_list = []
    buser = load_buser()

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
                    'app_id': appinfo['C_APP_ID'],
                    'overdue_days': appinfo['OVERDUE_DAYS'],
                    'app_status': appinfo['N_APP_STATUS']
                }
            if mxinfo != None:
                for contact in mxinfo['data']:
                    if not oddphone_filter(contact['phone_num']):
                        # 通讯关系
                        if not mobile_dict.__contains__(contact['phone_num']):
                            contact_user = {'mobile:ID': contact['phone_num']}
                            if buser.__contains__(contact['phone_num']):
                                contact_user['buser'] = 1
                                contact_user['buser_name'] = buser[contact['phone_num']]
                            mobile_dict[str(contact['phone_num'])] = contact_user
                        if contact['call_in_cnt'] > 0:
                            mobile_contact_list.append({
                                ":END_ID": user_phone,
                                ":START_ID": contact['phone_num'],
                                ':TYPE': 'CONTACTS'
                            })
                        if contact['call_out_cnt'] > 0:
                            mobile_contact_list.append({
                                ":END_ID": contact['phone_num'],
                                ":START_ID": user_phone,
                                ':TYPE': 'CONTACTS'
                            })

            print('------')

        # 测试用语句
        # if ran_dir_num > 9:
        #     break

    export_path = root_path + '/csv'
    with open(export_path + '/mobile.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['mobile:ID', 'app_id', 'idno', 'overdue_days', 'app_status', 'buser', 'buser_name'])
        f_csv.writeheader()
        f_csv.writerows(mobile_dict.values())
    with open(export_path + '/mobile_contact_relation.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(mobile_contact_list)


def load_buser():
    buser = {}

    with open('/home/fred/Documents/neo4j/BAPP_USER.csv', 'r') as f:
        f_csv = csv.reader(f)
        for line in f_csv:
            buser[line[4]] = line[2]

    return buser


def instinct_contact():
    res = {}
    export_path = root_path + '/csv'

    i = 0
    with open(export_path + '/mobile_contact_relation.csv', 'r') as f:
        fcsv = csv.DictReader(f)
        for line in fcsv:
            print(i)
            i += 1

            if not (res.__contains__(line[':START_ID'] + line[':END_ID']) or res.__contains__(
                    line[':END_ID'] + line[':START_ID'])):
                res[line[':START_ID'] + line[':END_ID']] = line

    with open(export_path + '/mobile_contact_relation2.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(res.values())


def test_mx_files(path):
    i = 0
    j = 0

    for root, dirs, files in os.walk(path):
        i = i + 1
        for file in files:
            if file.startswith('mx'):
                j = j + 1
                print('folder', i, 'file', j)


def test_strange_phone():
    i = 0
    phone_list = set()

    for root, dirs, files in os.walk(root_path + '/contacts/contact_list/'):
        for file in files:
            i = i + 1
            with open(root + file, 'r') as f:
                data = json.load(fp=f)

            for item in data['data']:
                phone = item['phone_num']
                if len(phone) != 11 or not phone.startswith('1'):
                    phone_list.add(phone)

            # break
            print('file num', i)

    with open(root_path + '/contacts/odd_phone.json', 'w') as f:
        json.dump(list(phone_list), f, cls=CustomEncoder, ensure_ascii=False)


def test_strange_phone_filter():
    with open(root_path + '/contacts/odd_phone_gte11.json', 'r') as f:
        data = json.load(fp=f)

    phone_list = []

    for phone in data:
        if not oddphone_filter(phone):
            phone_list.append(phone)

    print('total', len(data), 'left', len(phone_list))
    # print(phone_list)

    # with open(root_path + '/contacts/odd_phone_lt6.json', 'w') as f:
    #     json.dump(phone_lt6, f, cls=CustomEncoder, ensure_ascii=False)


if __name__ == '__main__':
    # graph_wash(root_path + '/raw')
    instinct_contact()
