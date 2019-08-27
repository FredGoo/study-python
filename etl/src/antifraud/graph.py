# -*- coding: utf-8 -*-
import csv
import json
import os

from py2neo import Graph, NodeMatcher

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190821'


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
            if row['Y'] == '1':
                matcher = NodeMatcher(test_graph)
                user = matcher.match("User", idno=row['C_CUST_IDNO']).first()
                if user != None:
                    user['overdue'] = 'Y'

                    tx = test_graph.begin()
                    tx.merge(user)
                    tx.graph.push(user)
                    tx.commit()


if __name__ == '__main__':
    mark_bad_user()
