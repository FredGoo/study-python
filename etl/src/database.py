# -*- coding: utf-8 -*-
import json

import pymysql


def connect_db(database):
    with open('json/env.json', 'r') as f:
        env = json.load(fp=f)

    return pymysql.connect(host=env['host'],
                           port=env['port'],
                           user=env['user'],
                           password=env['password'],
                           database=database,
                           charset='utf8')
