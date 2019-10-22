# -*- coding: utf-8 -*-
import json

import pymysql

from src.util.database import load_config, connect_db
from src.util.jsonlib import CustomEncoder

db_config = {}
path = '/home/fred/data/sample20190927/repayplan/'


def get_overdue_map():
    count_app_sql = '''
        select *
        from kezhi.app_id_list
        limit 9999999;
      '''
    con = connect_db(db_config['local'], 'kezhi')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(count_app_sql)
    applist = cur.fetchall()
    cur.close()
    con.close()

    i = 1
    for app in applist:
        appid = app['app_id']
        repayplanlist = get_repay_plan_by_appid(appid)
        with open(path + appid + '.json', 'w') as f:
            json.dump(repayplanlist, fp=f, cls=CustomEncoder)

        print(i)
        i += 1


def get_repay_plan_by_appid(appid):
    count_app_sql = '''
        select l.EXT_ID,
               l.TENOR,
               r.CURR_TENOR,
               r.PAY_DATE,
               r.FINISH_DATE
        from T_REPAY_PLAN r
                 left join T_LOAN l on r.LOAN_ID = l.ID
        where l.EXT_ID = '%s';
      '''
    con = connect_db(db_config['datacenter'], 'datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(count_app_sql % appid)
    repayplan = cur.fetchall()
    cur.close()
    con.close()

    return repayplan


if __name__ == '__main__':
    db_config = load_config()

    get_overdue_map()
