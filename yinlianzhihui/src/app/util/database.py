# -*- coding: utf-8 -*-
import json

import pymysql


def load_config():
    with open('config/database.json', 'r') as f:
        config = json.load(fp=f)
    return config


def connect_db(config, database):
    return pymysql.connect(host=config['host'],
                           port=config['port'],
                           user=config['user'],
                           password=config['password'],
                           database=database,
                           charset='utf8')
