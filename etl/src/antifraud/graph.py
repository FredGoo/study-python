# -*- coding: utf-8 -*-
import csv
import json
import os

import pymysql
from py2neo import Graph, NodeMatcher

from src.util.database import load_config, connect_db

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190821'
community_json_path = '/home/fred/git/study-python/etl/out/graph/taged.json'
mobile_json_path = '/home/fred/git/study-python/etl/out/graph/mobile2app.json'
mobile_res_path = '/home/fred/git/study-python/etl/out/graph/res.json'


def delete_all():
    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")
    test_graph.delete_all()


def export_data_csv():
    i = 0
    path = root_path + '/mx/contact_list'
    user_list = []
    mobile_list = set()
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
                mobile_list.add(str(json_data['phone']))
                # 手机归属
                # mobile_user_list.append({
                #     ":END_ID": str(json_data['idno']),
                #     ":START_ID": str(json_data['phone']),
                #     ':TYPE': 'BELONGS_TO'
                # })

                for contact in json_data['data']:
                    # 通讯关系
                    mobile_list.add(contact['phone_num'])
                    # mobile_contact_list.append({
                    #     ":END_ID": str(json_data['phone']),
                    #     ":START_ID": contact['phone_num'],
                    #     ':TYPE': 'CONTACTS'
                    # })

            # break

    export_path = root_path + '/mx/neo4j/contact_csv'
    # with open(export_path + '/user.csv', 'w', newline='')as f:
    #     f_csv = csv.DictWriter(f, ['idno:ID', 'name_cn'])
    #     f_csv.writeheader()
    #     f_csv.writerows(user_list)
    with open(export_path + '/mobile.csv', 'w', newline='')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(['mobile:ID'])
        for row in mobile_list:
            f_csv.writerow([row])
    # with open(export_path + '/user_mobile_relation.csv', 'w', newline='')as f:
    #     f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
    #     f_csv.writeheader()
    #     f_csv.writerows(mobile_user_list)
    # with open(export_path + '/mobile_contact_relation.csv', 'w', newline='')as f:
    #     f_csv = csv.DictWriter(f, [':START_ID', ':END_ID', ':TYPE'])
    #     f_csv.writeheader()
    #     f_csv.writerows(mobile_contact_list)


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
    RETURN algo.asNode(nodeId).mobile AS mobile, community
    ORDER BY community
    ''')

    data = {}
    for r in r_list:
        c = r['community']
        m = r['mobile']
        if not data.__contains__(c):
            data[c] = []

        data[c].append(m)

    with open(community_json_path, 'w') as f:
        json.dump(data, f)


def save_mobile_to_app():
    mobile2app = {}

    db_config = load_config()
    sql_str = "SELECT * from app_id_list where batch_id = 5"
    con = connect_db(db_config['local'], 'kezhi')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql_str)
    rows = cur.fetchall()
    cur.close()
    con.close()

    for row in rows:
        mobile2app[row['custom_mobile']] = {
            'appid': row['app_id'],
            'overdue': row['overdue_days']
        }

    with open(mobile_json_path, 'w') as f:
        json.dump(mobile2app, f)


def analysis_community():
    with open(mobile_json_path, 'r') as f:
        mobile2app = json.load(f)
        print('app in database', len(mobile2app.keys()))

    with open(community_json_path, 'r') as f:
        gcj = json.load(f)
        print('community num', len(gcj.keys()))

    i = 0
    res = {}
    community = 0
    for value in gcj.values():
        community_user = 0
        community_bad_user = 0

        for mobile in value:
            if mobile2app.__contains__(mobile):
                i = i + 1
                community_user = community_user + 1

                if mobile2app[mobile]['overdue'] > 0:
                    community_bad_user = community_bad_user + 1

        bad_rate = 0
        if community_user > 0:
            bad_rate = community_bad_user / community_user
        if bad_rate > 0.1:
            res[community] = {
                'total_user': community_user,
                'bad_user': community_bad_user,
                'mobile_list': value
            }

            community = community_bad_user + 1
            print('community user', community_user, 'bad rate', bad_rate)

    with open(mobile_res_path, 'w') as f:
        json.dump(res, f)

    print('mobile in app num', i)


def analysis_res():
    with open(mobile_res_path, 'r') as f:
        res = json.load(f)

    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")

    for community in res.values():
        if community['total_user'] > 500:
            print(community['total_user'])

            for mobile in community['mobile_list']:
                test_graph.run('match (n:Mobile{mobile:"%s"}) set n.community = "0"' % mobile)


if __name__ == '__main__':
    analysis_res()
