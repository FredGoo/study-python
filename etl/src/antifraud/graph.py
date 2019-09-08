# -*- coding: utf-8 -*-
import csv
import json
import os
from datetime import datetime

from py2neo import Graph, NodeMatcher

from src.util.phone import oddphone_filter

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190903'


def delete_all():
    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")
    test_graph.delete_all()


def export_data_csv():
    i = 0
    path = root_path + '/contacts/contact_list'
    user_list = []
    mobile_dict = {}
    mobile_user_list = []
    mobile_contact_list = []

    for root, dirs, files in os.walk(path):
        for file in files:
            i = i + 1
            print('file', i, file)

            with open(root + '/' + file, 'r') as f:
                json_data = json.load(f)

                # 用户节点
                # user_list.append({
                #     "idno:ID": str(json_data['idno']),
                #     "name_cn": json_data['name'],
                # })
                # 手机节点
                if not mobile_dict.__contains__(str(json_data['list'][0]['phone'])):
                    mobile_dict[str(json_data['list'][0]['phone'])] = {
                        'mobile:ID': str(json_data['list'][0]['phone']),
                        'idno': json_data['user']['C_CUST_IDNO'],
                        'app_id': json_data['user']['C_APP_ID'],
                        'overdue_days': json_data['user']['OVERDUE_DAYS'],
                        'app_status': json_data['user']['N_APP_STATUS']
                    }
                # 手机归属
                # mobile_user_list.append({
                #     ":END_ID": str(json_data['idno']),
                #     ":START_ID": str(json_data['phone']),
                #     ':TYPE': 'BELONGS_TO'
                # })

                for contact_data in json_data['list']:
                    for contact in contact_data['data']:
                        if not oddphone_filter(contact['phone_num']):
                            # 通讯关系
                            if not mobile_dict.__contains__(contact['phone_num']):
                                mobile_dict[str(contact['phone_num'])] = {
                                    'mobile:ID': contact['phone_num']
                                }

                            mobile_contact_list.append({
                                ":END_ID": str(json_data['list'][0]['phone']),
                                ":START_ID": contact['phone_num'],
                                ':TYPE': 'CONTACTS'
                            })

            # break

    export_path = root_path + '/contacts/neo4j/contact_csv'
    # with open(export_path + '/user.csv', 'w', newline='')as f:
    #     f_csv = csv.DictWriter(f, ['idno:ID', 'name_cn'])
    #     f_csv.writeheader()
    #     f_csv.writerows(user_list)
    with open(export_path + '/mobile.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['mobile:ID', 'app_id', 'idno', 'overdue_days', 'app_status'])
        f_csv.writeheader()
        f_csv.writerows(mobile_dict.values())
    # with open(export_path + '/user_mobile_relation.csv', 'w', newline='')as f:
    #     f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
    #     f_csv.writeheader()
    #     f_csv.writerows(mobile_user_list)
    with open(export_path + '/mobile_contact_relation.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
        f_csv.writeheader()
        f_csv.writerows(mobile_contact_list)


def mark_bad_user():
    path = '/home/fred/Documents/2.rmd/1.antifraud/医美欺诈用户数据.csv'
    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")

    with open(path) as f:
        f_csv = csv.DictReader(f)
        for row in f_csv:
            matcher = NodeMatcher(test_graph)
            user = matcher.match("User", idno=row['C_CUST_IDNO']).first()
            if user != None:
                if row['Y'] == '1':
                    user['overdue'] = 'Y'
                if row['Y'] == '0':
                    user['overdue'] = 'N'
                if row['Y'] == '-1':
                    user['overdue'] = 'U'

                print(user)
                tx = test_graph.begin()
                tx.merge(user)
                tx.graph.push(user)
                tx.commit()


def community_detection():
    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")
    r_list = test_graph.run('''
    CALL algo.louvain.stream('Mobile', 'CONTACTS', {})
    YIELD nodeId, community
    RETURN 
    algo.asNode(nodeId).mobile AS mobile, 
    algo.asNode(nodeId).app_id AS app_id,
    algo.asNode(nodeId).idno AS idno,
    algo.asNode(nodeId).overdue_days AS overdue_days,
    algo.asNode(nodeId).app_status AS app_status,
    community
    ORDER BY community
    ''')

    data = {}
    for r in r_list:
        c = r['community']
        m = {
            'mobile': r['mobile'],
            'community': r['community']
        }
        if r['app_id'] != None:
            m['app_id'] = r['app_id']
        if r['idno'] != None:
            m['idno'] = r['idno']
        if r['overdue_days'] != None:
            m['overdue_days'] = r['overdue_days']
        if r['app_status'] != None:
            m['app_status'] = r['app_status']
        if not data.__contains__(c):
            data[c] = []
        data[c].append(m)

        # break

    with open(root_path + '/contacts/neo4j/taged.json', 'w') as f:
        json.dump(data, f)


def analysis_community():
    with open(root_path + '/contacts/neo4j/taged.json', 'r') as f:
        gcj = json.load(f)
        print('community num', len(gcj.keys()))

    res = []
    for key in gcj.keys():
        user = 0
        bad_user = 0
        reject_user = 0
        overdue_user = 0

        for community_value in gcj[key]:
            if community_value.__contains__('app_id'):
                user += 1
                if community_value.__contains__('overdue_days'):
                    if int(community_value['overdue_days']) > 0:
                        overdue_user += 1
                        bad_user += 1
                if community_value.__contains__('app_status'):
                    if community_value['app_status'] == '140':
                        reject_user += 1
                        bad_user += 1

        community_item = {
            'community_key': key,
            'total_user_num': user,
            '140_user': reject_user,
            'overdue_user': overdue_user
        }
        print(community_item)
        res.append(community_item)

    with open(root_path + '/contacts/neo4j/community.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['community_key', 'total_user_num', '140_user', 'overdue_user'])
        f_csv.writeheader()
        f_csv.writerows(res)


def get_community_by_key():
    # key_list = ['90', '166', '137', '156', '186', '203', '184', '333', '93', '120']
    key_list = ['137', '156', '186', '203', '184', '333', '93', '120']

    with open(root_path + '/contacts/neo4j/taged.json', 'r') as f:
        gcj = json.load(f)

    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")
    set_str = '''
    match(m:Mobile{mobile:'%s'}) set m.community = '%s'
    '''

    for key in key_list:
        print('key', key)
        print('mobile num', len(gcj[key]))
        for unit in gcj[key]:
            if unit.__contains__('app_id'):
                print(unit['app_id'])

            test_graph.run(set_str % (unit['mobile'], key))


if __name__ == '__main__':
    start_time = datetime.now()

    # export_data_csv()
    # community_detection()
    # analysis_community()
    get_community_by_key()

    end_time = datetime.now()
    print('start', start_time, 'end', end_time, 'take', end_time - start_time)
