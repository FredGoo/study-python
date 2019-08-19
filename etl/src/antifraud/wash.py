# -*- coding: utf-8 -*-
import json


# 魔蝎
def mx(path):
    with open(path, 'r') as f:
        data = json.load(fp=f)

        # 报告有效性检查
        if not data['success']:
            return
        if not data.__contains__('report_data'):
            return

        report = data['report_data']
        mx_data = {}

        # behavior_check
        with open('../out/mx.json', 'r') as f:
            mxjson = json.load(fp=f)

        field_map['mx']['behavior_check'] = {}
        for behavior_check in report['behavior_check']:
            field_map['mx']['behavior_check'][behavior_check['check_point']] = {
                'key': 'xxx',
                "deal": 0
            }

        return mx_data


if __name__ == '__main__':
    mx_data = mx('/home/fred/git/study-python/etl/out/raw/NYB01-181204-200610/mx_1.json')
    print(mx_data)
