# -*- coding: utf-8 -*-
import json
import os

from py2neo import Graph, Node

root_path = '/home/fred/Documents/2.rmd/1.antifraud/out/data20190821'


def delete_all():
    test_graph = Graph("http://127.0.0.1:7474", username="neo4j", password="123")
    delete_res = test_graph.delete_all()
    print(delete_res)


if __name__ == '__main__':
    # delete_all()

    graph_connect = Graph("http://127.0.0.1:7474", username="neo4j", password="123")

    i = 0
    path = root_path + '/mx/collection_contact'
    for root, dirs, files in os.walk(path):
        for file in files:
            i = i + 1
            print('file', i, file)

            with open(root + '/' + file, 'r') as f:
                json_data = json.load(f)
                # graph_connect.create(Node('user', name=json_data['name'], idno=str(json_data['idno'])))
                for contact in json_data['data']:
                    for contact_detail in contact['contact_details']:
                        graph_connect.create(
                            Node('mobile', mobile=contact_detail['phone_num'], name=contact['contact_name'],
                                 location=contact_detail['phone_num_loc']))
