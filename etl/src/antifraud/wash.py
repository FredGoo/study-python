# -*- coding: utf-8 -*-
import datetime
import decimal
import json
import os

from src.antifraud.thirdparty import mx

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190821'


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
    i = 0
    j = 0

    for root, dirs, files in os.walk(path):
        i = i + 1
        for file in files:
            if file.startswith('mx'):
                j = j + 1
                print('folder', i, 'file', j)
                file_path = root + '/' + file

                res = mx.wash_contact_list(file_path)
                if res == 0:
                    return
                with open(root_path + '/mx/contact_list/' + res['idno'] + '.json', 'w') as f:
                    json.dump(res, f, cls=CustomEncoder, ensure_ascii=False)


if __name__ == '__main__':
    mx_map = load_mx_map()

    graph_wash(root_path + '/raw')
