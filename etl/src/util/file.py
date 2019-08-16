# -*- coding: utf-8 -*-
import datetime
import decimal
import json
import os


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        return super(CustomEncoder, self).default(obj)


def save_batch_json(data, folder_field, output_path, file_name):
    for data_item in data:
        save_json(data_item, data_item[folder_field], output_path, file_name)


def save_json(data_item, sub_folder, output_path, file_name):
    # 写入三方数据
    path = output_path + '/' + sub_folder

    # 创建文件夹
    folder_exists = os.path.exists(path)
    if not folder_exists:
        os.makedirs(path)

    with open(path + '/' + file_name + '.json', 'w') as f:
        json.dump(data_item, f, cls=CustomEncoder, ensure_ascii=False)
