# -*- coding: utf-8 -*-

# 魔蝎
import json

import pymysql

from src.util.database import connect_db


def wash(path, mx_map):
    print(path)
    f = open(path, "r")
    json_str = json.load(f)
    f.close()
    data = json.loads(json_str)

    # 报告有效性检查
    if not data['success']:
        return 0
    if not data.__contains__('report_data'):
        return 0

    report = data['report_data']
    mx_data = {}

    # behavior_check
    behavior_map = mx_map['behavior_check']
    behavior_check = report['behavior_check']
    for behavior in behavior_check:
        check_point = behavior['check_point']

        if not behavior_map.__contains__(check_point):
            print('behavior_check', check_point)
            print(behavior_check)
            return 0

        behavior_check_key = behavior_map[check_point]
        if behavior_check_key == 'behavior_check_ylxrhdqk':
            if behavior['result'] == '经常联系':
                behavior_check_key = behavior_check_key + '_jclx'
            elif behavior['result'] == '无联系记录':
                behavior_check_key = behavior_check_key + '_wlxjl'
            else:
                # 需要单独处理联系情况
                print(behavior)

        mx_data[behavior_check_key] = behavior['evidence']

    # cell_behavior
    cell_behavior = report['cell_behavior']
    if len(cell_behavior) != 1:
        print('cell_behavior', cell_behavior)
        return 0
    for behavior in cell_behavior[0]['behavior']:
        for key in behavior.keys():
            if not key == 'cell_mth':
                mx_data['cell_behavior_' + behavior['cell_mth'] + '_' + key] = behavior[key]

    # contact_region
    # todo

    # contact_list
    # todo

    # main_service
    # todo 需要单独分析
    main_service = report['main_service']
    # if len(main_service) > 1:
    #     print('main_service', main_service)
    #     return 0

    # deliver_address
    # todo
    deliver_address = report['deliver_address']
    if len(deliver_address) > 0:
        print('deliver_address', deliver_address)
        return 0

    # ebusiness_expense
    # todo
    ebusiness_expense = report['ebusiness_expense']
    if len(ebusiness_expense) > 0:
        print('ebusiness_expense', ebusiness_expense)
        return 0

    # person
    person = report['person']
    for person_key in person.keys():
        mx_data['person_' + person_key] = person[person_key]

    # application_check
    application_map = mx_map['application_check']
    application_check = report['application_check']
    for application in application_check:
        check_point = application['check_point']

        if not application_map.__contains__(check_point):
            print('application_check', check_point)
            print(application_check)
            return 0

        application_check_key = application_map[check_point]
        mx_data[application_check_key] = application['result']

    return mx_data


def wash_trip_info(path):
    print(path)
    f = open(path, "r")
    json_str = json.load(f)
    f.close()
    data = json.loads(json_str)

    # 报告有效性检查
    if not data['success']:
        return 0
    if not data.__contains__('report_data'):
        return 0

    report = data['report_data']

    # trip_info
    # todo


# 联系人名单
def wash_collection_contact(path):
    print(path)
    f = open(path, "r")
    data = json.load(f)
    f.close()

    # 报告有效性检查
    if not data['success']:
        return 0
    if not data.__contains__('report_data'):
        return 0

    report = data['report_data']

    # collection_contact
    collection_contact = report['collection_contact']

    if len(report['data_source']) > 1:
        print(report['data_source'])
        print('data_source > 1')

    return {
        'data': collection_contact,
        'idno': report['person']['id_card_num'],
        'name': report['person']['real_name'],
        'phone': report['data_source'][0]['account']
    }


# 运营商联系人通话详情
def wash_contact_list(path):
    print(path)
    f = open(path, "r")
    data = json.load(f)
    f.close()

    # 报告有效性检查
    if not data['success']:
        return 0
    if not data.__contains__('report_data'):
        return 0

    report = data['report_data']

    # contact_list
    contact_list = report['contact_list']

    if len(report['data_source']) > 1:
        print(report['data_source'])
        print('data_source > 1')

    return {
        'data': contact_list,
        'idno': report['person']['id_card_num'],
        'name': report['person']['real_name'],
        'phone': report['data_source'][0]['account']
    }


def fetch_batch(mbl_list_str, db_config):
    # gravity库
    sql_str = "select mt.MOBILE as ckey, mr.REPORT as cdata, mt.ID as cid from GEEX_MX_TASK mt left join GEEX_MX_REPORT mr on mt.TASK_ID = mr.TASK_ID where mt.N_SUCCESS in (1, 2) and mt.MOBILE in (%s) and mr.REPORT is not null and mr.REPORT <> '' " \
              % mbl_list_str

    con = connect_db(db_config, 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    res = cur.fetchall()
    cur.close()
    con.close()

    return res
