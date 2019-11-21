# -*- coding: utf-8 -*-
import codecs
import csv
import json
import smtplib
from datetime import datetime, timedelta
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import Levenshtein
import pymysql

type_map = {
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
template_map = []


def connect_mysql_db(database):
    with open('config/database.json', 'r') as f:
        database_config = json.load(fp=f)

    return pymysql.connect(host=database_config['host'],
                           port=database_config['port'],
                           user=database_config['user'],
                           password=database_config['password'],
                           database=database,
                           charset='utf8')


def query_sms_result(start, end):
    res = {}
    header = ['渠道',
              '类型',
              '总数',
              '送达',
              '其他',
              '未知']

    sql = '''
    select * from GEEX_SMS_RECORD r left join GEEX_SMS_REPORT p on r.TASK_ID = p.SEQ_ID where r.D_CREATE between '%s' and '%s' limit 1
    ''' % (start, end)

    con = connect_mysql_db('datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    for row in rows:
        # 寻找短信模板
        sendmsgcontent = row['C_MSG']
        print('发送短信内容', sendmsgcontent)

        mindis = 99999
        templaterow = {}
        for template in template_map:
            templatecontent = template['C_CONTENT']

            dis = Levenshtein.distance(sendmsgcontent, templatecontent)
            if dis < mindis:
                mindis = dis
                templaterow = template

        print('数据库中模板', templaterow['C_CONTENT'], '\n')

        # 封装返回
        bustype = row['MSG_TYPE']
        if templaterow['N_BUSINESS_ID'] != None and type_map.__contains__(templaterow['N_BUSINESS_ID']):
            msgtype = type_map[templaterow['N_BUSINESS_ID']]
        else:
            msgtype = templaterow['N_BUSINESS_ID']
        smsstate = row['STATE']
        if smsstate == None or smsstate == '':
            smsstate = '未知'

        if bustype != None and msgtype != None:
            if not res.__contains__(bustype + msgtype):
                res[bustype + msgtype] = {
                    '渠道': bustype,
                    '类型': msgtype,
                    '总数': 0,
                    '送达': 0,
                    '其他': 0,
                    '未知': 0
                }
            if not res[bustype + msgtype].__contains__(smsstate):
                res[bustype + msgtype][smsstate] = 1
            else:
                res[bustype + msgtype][smsstate] += 1

            # 归类
            if smsstate == 'DELIVRD':
                res[bustype + msgtype]['送达'] += 1
            elif smsstate != '未知':
                res[bustype + msgtype]['其他'] += 1

            # 总数
            res[bustype + msgtype]['总数'] += 1

            # 文件头
            if not header.__contains__(smsstate):
                header.append(smsstate)

    return res, header


def load_sms_vendoer_map():
    sql = '''
        select * from GEEX_SMS_TEMPLATE;
    '''

    con = connect_mysql_db('datacenter')
    cur = con.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    con.close()

    return rows


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def sendemail(header, data):
    csv_file = codecs.open('sms.csv', 'w', 'gbk')
    csvwriter = csv.DictWriter(csv_file, header)
    csvwriter.writeheader()
    csvwriter.writerows(data.values())
    csv_file.close()

    # 测试
    # return

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
                    [to_addr],
                    # ['wujiadi@geexfinance.com', 'wangjilu@geexfinance.com', 'jixinyang@geexfinance.com', to_addr],
                    msg.as_string())
    server.quit()


if __name__ == '__main__':
    template_map = load_sms_vendoer_map()

    dt = datetime.today()
    end = dt.strftime('%Y-%m-%d')
    startday = dt - timedelta(days=1)
    start = startday.strftime('%Y-%m-%d')

    res, header = query_sms_result(start, end)

    sendemail(header, res)
