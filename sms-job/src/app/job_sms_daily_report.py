# -*- coding: utf-8 -*-
import csv
import json
import smtplib
import xml.etree.ElementTree as ET
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import pymysql
import requests

es_host = 'http://192.168.100.100:9200'
es_url = 'http://192.168.100.100:9200/logstash-2019.11.10/_search'
headers = {'Content-Type': 'application/json'}
auth = ('itguest', 'Geex2016')
smsmap = {
    1: "超G会员",
    2: "超即花",
    3: "催收提醒",
    4: "反欺诈提醒",
    5: "还款方式",
    6: "还款提醒",
    7: "审批短信",
    8: "现金贷",
    9: "验证码"
}

huaxin_config = {
    'tongzhi': {
        'url': 'http://dx.ipyy.net/statusApi.aspx',
        'userid': '60506',
        'account': '9B000153',
        'password': 'C1879E192918B907B508E926224DF8A2',
    },
    # 'yingxiao': {
    #     'userid': '',
    #     'account': '9BYX00117',
    #     'password': '',
    # },
    'yanzhengma': {
        'url': 'http://dx.ipyy.net/statusApi.aspx',
        'userid': '60506',
        'account': '9B000153',
        'password': 'C1879E192918B907B508E926224DF8A2',
    },
    'wangdai': {
        'url': 'http://dx110.ipyy.net/statusApi.aspx',
        'userid': '7199',
        'account': '9BWD00036',
        'password': '4CE6B7F65C369BE4EA123EA1F22FBE8F',
    },
    'cuishou': {
        'url': 'http://dx110.ipyy.net/statusApi.aspx',
        'userid': '7198',
        'account': '9BYX00107',
        'password': 'DFC3B6AC521781C98886F95504523F9B',
    }
}
huaxin_map = {}


def connect_mysql_db(database):
    with open('config/database.json', 'r') as f:
        database_config = json.load(fp=f)

    return pymysql.connect(host=database_config['host'],
                           port=database_config['port'],
                           user=database_config['user'],
                           password=database_config['password'],
                           database=database,
                           charset='utf8')


def query_huaxin_es():
    smscsv = {}

    # 根据华信条件获取日志
    query = '''
{
	"query": {
		"bool": {
			"must": [{
					"match": {
						"app_name": "MessagePusher"
					}
				},
								{
					"match": {
						"message": "HuaxinSmsService"
					}
				},
				{
					"match": {
						"message": "taskID"
					}
				}
			]
		}
	},
    "_source": {
        "includes": [
            "thread",
            "message",
            "timestamp",
            "host_name"
        ]
    },
    "sort" : ["_doc"], 
    "size": 100
}
    '''
    res = requests.post(url=es_url + '?scroll=10m', headers=headers, data=query, auth=auth)
    resJson = json.loads(res.content)
    scrollid = resJson['_scroll_id']
    print(resJson)

    # 根据线程号获取短信内容
    for hit in resJson['hits']['hits']:
        smscsv = process_huaxin_hit(hit, smscsv)

    # scroll遍历
    breakflag = True
    while breakflag:
        scrollquery = '{"scroll": "10m", "scroll_id": "%s"}' % scrollid
        scrollres = requests.post(url=es_host + '/_search/scroll', headers=headers, data=scrollquery, auth=auth)
        scrollresJson = json.loads(scrollres.content)
        scrollid = scrollresJson['_scroll_id']
        print(scrollresJson)

        # 根据线程号获取短信内容
        for hit in scrollresJson['hits']['hits']:
            smscsv = process_huaxin_hit(hit, smscsv)

        if len(scrollresJson['hits']['hits']) < 1:
            breakflag = False

    return smscsv


def process_huaxin_hit(hit, smscsv):
    print(hit['_source'])
    # 获取线程号
    thread_no = hit['_source']['thread'].split("#")[1].split("-")[1]
    # 获取taskid
    task_message = hit['_source']['message'].replace('HuaxinSmsService end: ', '')
    task_result = json.loads(task_message)
    taskid = task_result['taskID']
    if taskid == '':
        return smscsv

    # 获取hostname
    hostname = hit['_source']['host_name']

    query = '''
    {
    	"query": {
    		"bool": {
    			"must": [{
    					"match": {
    						"app_name": "MessagePusher"
    					}
    				},
    				{
    					"match": {
    						"thread": "%s"
    					}
    				},
    				{
    					"match": {
    						"host_name": "%s"
    					}
    				},
    				{
    					"range": {
    						"timestamp": {
    							"lt": %s
    						}
    					}
    				}
    			]
    		}
    	},
    	"sort": {
    		"timestamp": {
    			"order": "desc"
    		}
    	},
    	    "_source": {
            "includes": [
                "thread",
                "message"
            ]
        },
    	"size": 2
    }
            '''
    query = query % (thread_no, hostname, hit['_source']['timestamp'])
    res1 = requests.post(url=es_url, headers=headers, data=query, auth=auth)
    resJson1 = json.loads(res1.content)

    for hit1 in resJson1['hits']['hits']:
        if not hit1['_source']['message'].__contains__('channelConfig'):
            hit1message = hit1['_source']['message'].replace('Send Sms Message START: ', '')

            try:
                qresstatus, qresbusinessid = query_huaxin_by_taskid(taskid, json.loads(hit1message)['templateId'])
                if smscsv.__contains__(qresbusinessid) and smscsv[qresbusinessid].__contains__(qresstatus):
                    smscsv[qresbusinessid][qresstatus] += 1
                else:
                    smscsv[qresbusinessid] = {
                        qresstatus: 1
                    }
            except:
                print('process_huaxin_hit error', hit1['_source'])

    return smscsv


def query_huaxin_by_taskid(taskid, templateid):
    mapconfig = huaxin_map[templateid]
    queryconfig = huaxin_config[mapconfig['C_CHANNEL_ID']]
    data = {
        'userid': queryconfig['userid'],
        'account': queryconfig['account'],
        'password': queryconfig['password'],
        'statusNum': 1,
        'action': 'query',
        'taskid': taskid
        # 'taskid': '1911044500010583'
    }
    response = requests.get(url=queryconfig['url'], params=data)
    xml = ET.fromstring(response.content)
    if len(xml) > 0:
        status = xml[0][2].text
    else:
        status = 999
    return status, mapconfig['N_BUSINESS_ID']


def load_huaxin_sms_vendoer_map():
    sql = '''
        select C_TEMPLATE_ID, C_CHANNEL_ID, N_BUSINESS_ID from GEEX_SMS_TEMPLATE where N_BUS_TYPE = 8;
    '''

    con = connect_mysql_db('datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    for row in rows:
        huaxin_map[row['C_TEMPLATE_ID']] = row


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def sendemail(data):
    csv_file = open('sms.csv', 'w')
    csvwriter = csv.DictWriter(csv_file, ['渠道', '类型', '成功', '失败'])
    csvwriter.writeheader()

    for key in data.keys():
        csvrow = {
            '渠道': '华信',
            '类型': smsmap[key]
        }
        for keykey in data[key]:
            if keykey == '10':
                csvrow['成功'] = data[key][keykey]
            else:
                csvrow['失败'] = data[key][keykey]

        csvwriter.writerow(csvrow)

    csv_file.close()

    from_addr = 'noreply@geexfinance.com'
    pwd = 'Geexfcs1234'
    smtp_server = 'mail.geexfinance.com'
    to_addr = 'guye@geexfinance.com'

    msg = MIMEMultipart()
    msg['From'] = _format_addr('脚本 <%s>' % from_addr)
    msg['To'] = _format_addr('xx <%s>' % to_addr)
    msg['Subject'] = Header('短信统计报告', 'utf-8').encode()

    # 邮件正文是MIMEText:
    msg.attach(MIMEText('fyi', 'plain', 'utf-8'))

    # 添加附件就是加上一个MIMEBase，从本地读取一个图片:
    att1 = MIMEText(open('sms.csv', 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename="{0}"'.format('sms.csv')
    msg.attach(att1)

    server = smtplib.SMTP(smtp_server, 587)
    server.starttls()
    server.login(from_addr, pwd)
    server.sendmail(from_addr,
                    ['wujiadi@geexfinance.com', 'wangjilu@geexfinance.com', 'jixinyang@geexfinance.com', to_addr],
                    msg.as_string())
    server.quit()


if __name__ == '__main__':
    load_huaxin_sms_vendoer_map()
    hxres = query_huaxin_es()
    sendemail(hxres)
