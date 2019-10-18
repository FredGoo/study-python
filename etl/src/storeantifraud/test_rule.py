# -*- coding: utf-8 -*-
import csv
import json
import os
import random
from datetime import datetime

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


def get_app_test_data(num):
    sample = [
        {
            'C_APP_ID': 'NYB01-181226-407705',
            'C_ORG04': 'XJDL0002',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180702-266746',
            'C_ORG04': 'XJDL0004',
            'OVERDUE_DAYS': 100
        },
        {
            'C_APP_ID': 'NYB01-180412-409423',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180813-566405',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180425-131082',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180330-161386',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180706-171875',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180515-361969',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        },
        {
            'C_APP_ID': 'NYB01-180902-184258',
            'C_ORG04': 'aaa',
            'OVERDUE_DAYS': 0
        }
    ]

    return random.sample(sample, num)


def get_app_prd(samplenum, rate):
    tmppath = '/home/fred/git/study-python/etl/out/store/totalapp.json'
    if not os.path.exists(tmppath):
        count_app_sql = '''
            select ac.C_APP_ID, ac.C_ORG04, tl.STORE_NAME, tl.OVERDUE_DAYS
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
        with open(tmppath, 'w')as f:
            json.dump(applist, fp=f)

    with open(tmppath, 'r') as f:
        appjson = json.load(f)
        print('放款订单总数', len(appjson))
        if samplenum < 1:
            samplenum = int(len(appjson) * rate)
        print('样本数', samplenum)
        return random.sample(appjson, samplenum)


def fetch_app(storelist, test, absnum, ratenum):
    # 欺诈门店
    fraudstorelist = {}
    for store in storelist:
        fraudstorelist[store['C_STORE_CODE']] = store['C_NAME']

    if test:
        appsample = get_app_test_data(1)
    else:
        appsample = get_app_prd(absnum, ratenum)
    global csvsize
    csvsize = len(appsample)

    rulemap = {}
    rulemapdetail = {}
    processtime = 0
    i = 1
    for appitem in appsample:
        print('订单', i, appitem['C_APP_ID'])
        i += 1

        resjson = post_app_group(appitem['C_APP_ID'])
        processtime += resjson['result']['processTime']

        # 分析命中规则
        for rule in resjson['result']['ruleMap'].values():
            # 规则详情报表
            if not rulemapdetail.__contains__(rule['key']):
                rulemapdetail[rule['key']] = []
            rulemapdetail[rule['key']].append({
                'appId': appitem['C_APP_ID'],
                'store': appitem['STORE_NAME'],
                'storeLevel': 'Y' if fraudstorelist.__contains__(appitem['C_ORG04']) else 'N',
                'overdueDays': appitem['OVERDUE_DAYS'],
                'hitNum': rule['triggerNum'],
                'matchedApplicationModelList': rule['matchedApplicationModelList']
            })

            # 统计数据
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

    return rulemap, rulemapdetail, processtime / 1000


def post_app_group(appid):
    # 查询订单信息
    res = es.get(index="appinfo_prd", doc_type='_doc', id=appid)

    # 计算订单命中情况
    url = 'http://127.0.0.1:8080/geex-dolphin-provider/calculate/group/1/temporary'
    d = {'map': json.dumps(res['_source'])}
    r = requests.post(url, data=d)
    return json.loads(r.text)


def test_app_single(appid):
    start_time = datetime.now()

    res = post_app_group(appid)
    print(res)

    end_time = datetime.now()
    print('start', start_time, 'take', end_time - start_time)


def test_app_batch(test, fix, rate):
    start_time = datetime.now()

    storelist = fetch_store_by_level(['9Z', '9F'])
    rulemap, rulemapdetail, processtime = fetch_app(storelist, test, fix, rate)

    end_time = datetime.now()
    print('start', start_time, 'take', end_time - start_time)

    # 落地
    path = '/home/fred/git/study-python/etl/out/store/' + str(csvsize) + '/'
    if not os.path.exists(path):
        os.mkdir(path)
    with open(path + 'rules_app.csv', 'w', newline='')as f:
        f_csv = csv.DictWriter(f, ['key', 'hit', 'overdue90_hit', 'fraud_store_hit', 'fraud_store_overdue90_hit'])
        f_csv.writeheader()
        f_csv.writerows(rulemap.values())
    for rulekey in rulemapdetail:
        rulepath = path + rulekey + '/'
        if not os.path.exists(rulepath):
            os.mkdir(rulepath)
        with open(rulepath + rulekey + '.csv', 'w', newline='') as f:
            f_csv = csv.DictWriter(f,
                                   ['appId', 'store', 'storeLevel', 'overdueDays', 'hitNum',
                                    'matchedApplicationModelList'])
            f_csv.writeheader()
            f_csv.writerows(rulemapdetail[rulekey])

    dolphintps = csvsize / processtime
    print('dolphin tps', round(dolphintps, 1), '还差目标(60+)' + str(round(60 / dolphintps, 1)) + '倍')

    tps = csvsize / (end_time - start_time).total_seconds()
    print('test unit tps', round(tps, 1), '还差目标(60+)' + str(round(60 / tps, 1)) + '倍')


if __name__ == '__main__':
    # 加载数据库配置
    db_config = load_config()

    # test_app_single('NYB01-180325-816452')
    test_app_batch(0, 3000, 0.05)
