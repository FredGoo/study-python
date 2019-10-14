# -*- coding: utf-8 -*-
import csv
import json
import random

import pymysql
import requests
from elasticsearch import Elasticsearch

from src.util.database import load_config, connect_db

db_config = {}
es = Elasticsearch()
csvsize = 0


def fetch_store_by_level(levellist):
    count_app_sql = '''
        select C_STORE_CODE, C_NAME
        from BIZ_STORE_ORGNZ
        where C_LEVEL in ('%s');
      '''
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(count_app_sql % "', '".join(levellist))
    storelist = cur.fetchall()
    cur.close()
    con.close()

    return storelist


def fetch_app(storelist):
    # 欺诈门店
    fraudstorelist = {}
    for store in storelist:
        fraudstorelist[store['C_STORE_CODE']] = store['C_NAME']

    count_app_sql = '''
        select ac.C_APP_ID, ac.C_ORG04, tl.OVERDUE_DAYS
        from BIZ_APP_COMMON ac
                 left join T_LOAN tl on ac.C_APP_ID = tl.EXT_ID
        where D_CREATE between '2017-01-01' and '2019-01-01'
          and N_APP_STATUS = 160;
      '''
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(count_app_sql)
    applist = cur.fetchall()
    cur.close()
    con.close()
    print('放款订单总数', len(applist))

    samplenum = int(len(applist) * 0.05)
    global csvsize
    csvsize = samplenum
    print('样本数', samplenum)
    appsample = random.sample(applist, samplenum)
    # 测试
    # appsample = [
    #     {
    #         'C_APP_ID': 'NYB01-181226-407705',
    #         'C_ORG04': 'XJDL0002',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180702-266746',
    #         'C_ORG04': 'XJDL0004',
    #         'OVERDUE_DAYS': 100
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180412-409423',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180813-566405',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180425-131082',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180330-161386',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180706-171875',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180515-361969',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     },
    #     {
    #         'C_APP_ID': 'NYB01-180902-184258',
    #         'C_ORG04': 'aaa',
    #         'OVERDUE_DAYS': 0
    #     }
    # ]

    rulemap = {}
    i = 1
    for appitem in appsample:
        print('订单', i, appitem['C_APP_ID'])
        i += 1

        # 查询订单信息
        appid = appitem['C_APP_ID']
        res = es.get(index="appinfo_prd", doc_type='_doc', id=appid)

        # 计算订单命中情况
        url = 'http://127.0.0.1:8080/geex-dolphin-provider/calculate/group/1'
        d = {'map': json.dumps(res['_source'])}
        r = requests.post(url, data=d)
        resjson = json.loads(r.text)

        # 分析命中规则
        for rule in resjson['result']['ruleMap'].values():
            rulesanalysis = {
                'key': rule['key'],
                'hit': 0,
                'overdue90_hit': 0,
                'fraud_store_hit': 0,
                'fraud_store_overdue90_hit': 0
            }
            if rulemap.__contains__(rule['key']):
                rulesanalysis = rulemap[rule['key']]

            # 计算命中
            rulesanalysis['hit'] += 1
            if appitem['OVERDUE_DAYS'] > 90: rulesanalysis['overdue90_hit'] += 1
            if fraudstorelist.__contains__(appitem['C_ORG04']): rulesanalysis['fraud_store_hit'] += 1
            if appitem['OVERDUE_DAYS'] > 90 and fraudstorelist.__contains__(appitem['C_ORG04']):
                rulesanalysis['fraud_store_overdue90_hit'] += 1

            rulemap[rule['key']] = rulesanalysis

    return rulemap


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()

    storelist = fetch_store_by_level(['9Z', '9F'])
    rulemap = fetch_app(storelist)
    with open('/home/fred/git/study-python/etl/out/store/rules_app' + str(csvsize) + '.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['key', 'hit', 'overdue90_hit', 'fraud_store_hit', 'fraud_store_overdue90_hit'])
        f_csv.writeheader()
        f_csv.writerows(rulemap.values())
