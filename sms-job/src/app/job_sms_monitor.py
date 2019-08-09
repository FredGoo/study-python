# encoding:utf-8

import json
import os
from datetime import datetime, timedelta

import pymysql
import sqlalchemy
from sqlalchemy import Column, Integer, VARCHAR, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Monitor_Instrument_Panel(Base):
    __tablename__ = 'T_MONITOR_INSTRUMENT_PANEL'

    MIT_ID = Column(Integer, primary_key=True)
    MIT_NAME_ONE = Column(VARCHAR)
    MIT_NAME_TWO = Column(VARCHAR)
    MIT_NAME_CONTENT = Column(VARCHAR)
    MIT_CYCLE = Column(VARCHAR)
    MIT_STATUS = Column(VARCHAR)
    BEGIN_TIME = Column(DATE)
    END_TIME = Column(DATE)

    def __str__(self):
        return 'id: %d, name_one: %s, name_two: %s, content: %s, cycle: %s, status: %s, begin: %s, end: %s' % \
               (self.MIT_ID, self.MIT_NAME_ONE, self.MIT_NAME_TWO, self.MIT_NAME_CONTENT, self.MIT_CYCLE,
                self.MIT_STATUS, self.BEGIN_TIME, self.END_TIME)


def connect_mysql_db(database):
    with open('config/database.json', 'r') as f:
        database_config = json.load(fp=f)

    return pymysql.connect(host=database_config['host'],
                           port=database_config['port'],
                           user=database_config['user'],
                           password=database_config['password'],
                           database=database,
                           charset='utf8')


def connect_oracle_db():
    os.environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"
    engine_save = sqlalchemy.create_engine('oracle+cx_oracle://risk:risk@192.168.4.75:1521/rsda',
                                           convert_unicode=False, echo=False, encoding='utf-8')
    DBSession = sessionmaker(bind=engine_save)
    return DBSession()


# 获取一小时内的安卓订单
def get_orders(start):
    delta = timedelta(hours=-1)
    pre_date = start + delta
    start_date = datetime.strftime(pre_date, '%Y-%m-%d %H:%M:%S')
    sql = "select common.C_APP_ID, norm.C_VAR701, common.C_OS_TYPE, common.D_CREATE \
                    from BIZ_APP_COMMON common \
                    left join BIZ_APP01 norm on common.C_APP_ID = norm.C_APP_ID \
                    where norm.C_VAR701 = 'geexcapp' \
                    and common.D_CREATE > '" + start_date + "' \
                    and (common.C_OS_TYPE < 'b' or common.C_OS_TYPE is null or common.C_OS_TYPE = '')\
                    order by common.D_CREATE desc"

    con = connect_mysql_db('datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    # 获取订单号的集合
    app_ids = []
    for row in rows:
        app_ids.append(row['C_APP_ID'])

    print("get_orders: %s" % app_ids)
    return app_ids


# 获取短信信息
def get_order_sms(app_id_list):
    if len(app_id_list) < 1:
        return []

    app_id_list1 = []
    for app_id in app_id_list:
        app_id_list1.append("'" + app_id + "'")
    app_ids_str = ','.join(app_id_list1)

    sql = "select APP_ID, count(1) as num \
                    from T_CUSTOM_SMS_INFO \
                    where APP_ID in (" + app_ids_str + ") \
                    group by APP_ID"

    con = connect_mysql_db('datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    app_sms = []
    for row in rows:
        app_sms.append(row['APP_ID'])

    print("get_order_sms: %s" % app_sms)
    return app_sms


# 比较缺失信息
def get_missing_order(app_ids, app_sms):
    return list(set(app_ids).difference(set(app_sms)))


# 记录监控信息
def save_record(id, name1, name2, content, status, cycle, start):
    end = datetime.now()
    panel = Monitor_Instrument_Panel(MIT_ID=id,
                                     MIT_NAME_ONE=name1,
                                     MIT_NAME_TWO=name2,
                                     MIT_NAME_CONTENT=content,
                                     MIT_CYCLE=cycle,
                                     MIT_STATUS=status,
                                     BEGIN_TIME=start,
                                     END_TIME=end)
    print("save_record, %s" % panel.__str__())

    session = connect_oracle_db()
    session.add(panel)
    session.commit()
    session.close()


if __name__ == '__main__':
    # 开始
    start = datetime.now()

    # 获取缺失短信的订单
    app_id_list = get_orders(start)
    app_sms_list = get_order_sms(app_id_list)
    diff_list = get_missing_order(app_id_list, app_sms_list)

    # 计算缺失率
    missing_rate = (len(diff_list) / len(app_id_list))

    # 记录统计信息
    warning_rate = 0.1
    error_rate = 0.2

    status = 0
    if missing_rate > error_rate:
        status = 2
    elif missing_rate > warning_rate:
        status = 1

    content = {
        'missing_rate': missing_rate,
        'missing_app': diff_list
    }

    save_record(21, '内部数据源监控', '短信异常监控', json.dumps(content), status, 'H', start)
