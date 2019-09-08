# -*- coding: utf-8 -*-
import datetime
import decimal
import json
import os

from src.antifraud.thirdparty import mx
from src.util.phone import oddphone_filter

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190903'


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
            print(mxinfo)

        # 测试用语句
        if ran_dir_num > 9:
            return


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
    graph_wash(root_path + '/raw')
