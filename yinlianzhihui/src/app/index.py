# -*- coding: utf-8 -*-
from src.app.util.database import load_config

db_config = {}
api_url = 'https://smarteye.unionpaysmart.com/'

if __name__ == '__main__':
    db_config = load_config()
    print(db_config)
