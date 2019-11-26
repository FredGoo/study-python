import prometheus_client
import pymysql
import requests
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry


# def connect_db():
#     return pymysql.connect(host='dc2.geexfinance.com',
#                            port=3306,
#                            user='geexdata',
#                            password='8D72b5a85-8c5306',
#                            database='datacenter',
#                            charset='utf8')
def connect_db():
    return pymysql.connect(host='dc2.geexfinance.com',
                           port=3306,
                           user='guye',
                           password='56b8M-BF4c1ba',
                           database='datacenter',
                           charset='utf8')


sql = '''
select r.MSG_TYPE, p.STATE, count(1) as num
from GEEX_SMS_RECORD r
         left join GEEX_SMS_REPORT p on r.TASK_ID = p.SEQ_ID
where r.D_CREATE > date_sub(date_format(now(),'%Y-%m-%d %H:%m:%s'),interval 1 hour) 
and r.MSG_TYPE in ('huaxin', 'emay_V')
group by r.MSG_TYPE, p.STATE;
'''
con = connect_db()
cur = con.cursor(cursor=pymysql.cursors.DictCursor)
cur.execute(sql)
rows = cur.fetchall()
cur.close()
con.close()

REGISTRY = CollectorRegistry(auto_describe=False)
instance = '#0'
input = Gauge("sms_send", instance, ['msg_type', 'state', 'instance'], registry=REGISTRY)

sms_stat = {}
for row in rows:
    msg_type = row['MSG_TYPE']
    if not sms_stat.__contains__(msg_type):
        sms_stat[msg_type] = {'msg_type': msg_type, 'total': 0, 'success': 0}
    if row['STATE'] == 'DELIVRD':
        sms_stat[msg_type]['success'] += row['num']

    sms_stat[msg_type]['total'] += row['num']

for sms_stat_item in sms_stat.values():
    total = sms_stat_item['total']
    success = sms_stat_item['success']
    failure = total - success
    input.labels(msg_type=sms_stat_item['msg_type'], state='total', instance=instance).set(total)
    input.labels(msg_type=sms_stat_item['msg_type'], state='success', instance=instance).set(success)
    input.labels(msg_type=sms_stat_item['msg_type'], state='failure', instance=instance).set(failure)

requests.post("http://192.168.4.202:9091/metrics/job/sms", data=prometheus_client.generate_latest(REGISTRY))
print("发送了一次短信")
