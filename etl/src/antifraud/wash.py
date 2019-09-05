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
    contact_info_body = {'list': [], 'user': {}}
    i = 0
    j = 0

    # 文件夹
    for root, dirs, files in os.walk(path):
        if len(contact_info_body['list']) > 0:
            with open(root_path + '/contacts/contact_list/' + contact_info_body['user']['C_APP_ID'] + '.json',
                      'w') as f:
                json.dump(contact_info_body, f, cls=CustomEncoder, ensure_ascii=False)
            contact_info_body = {'list': [], 'user': {}}

        print('----------------------------')
        print('files', files)
        i += 1

        # 每一个文件夹里的文件
        for file in files:
            file_path = root + '/' + file

            if file.startswith('appinfo'):
                with open(file_path) as f:
                    user_info = json.load(fp=f)
                contact_info_body['user'] = user_info
            if file.startswith('mx'):
                j += 1
                print('folder', i, 'file', j)

                res = mx.wash_contact_list(file_path)
                if res == 0:
                    return
                contact_info_body['list'].append(res)

            # if i == 10:
            #     return


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
