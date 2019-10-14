# -*- coding: utf-8 -*-
import hashlib
import json
import os
import re

rootdir = '/home/fred/Documents/2.rmd/2.kezhi/sample20190927/encryption'


def encode_mbl_idno_item(root, root_ec, file, hl):
    infile = open(root + '/' + file, "r", encoding="utf-8").read()
    # infile = '"report_time": 1543909041000,'
    new_infile = infile

    # 手机号加密
    mb_reg = '1[3|4|5|7|8][0-9]{9}'
    mb_match = re.findall(mb_reg, new_infile)
    if len(mb_match) > 0 and not file.__contains__('td'):
        for mb in mb_match:
            hl.update(mb.encode(encoding='utf-8'))
            new_infile = new_infile.replace(mb, hl.hexdigest())

    # 身份证号
    idno_reg = r'([1-9]\d{5}[12]\d{10}[0-9xX])'
    idno_match = re.findall(idno_reg, new_infile)
    if len(idno_match) > 0:
        for idno in idno_match:
            hl.update(idno.encode(encoding='utf-8'))
            new_infile = new_infile.replace(idno, hl.hexdigest())

    # 银行卡号
    bankcard_reg = re.compile(r'[1-9]\d{14}|[1-9]\d{18}')
    bankcard_match = bankcard_reg.findall(new_infile)
    if len(bankcard_match) > 0:
        for bankcard in bankcard_match:
            hl.update(bankcard.encode(encoding='utf-8'))
            new_infile = new_infile.replace(bankcard, hl.hexdigest())

    # 加密姓名
    if file == 'appinfo.json':
        print(root)
        appinfojson = json.loads(new_infile)
        name = appinfojson['C_NAME_CN']
        if name != None:
            hl.update(name.encode(encoding='utf-8'))
            appinfojson['C_NAME_CN'] = hl.hexdigest()
            new_infile = json.dumps(appinfojson)

    # 保存加密后的数据
    outfile = open(root_ec + '/' + file, "w", encoding="utf-8")
    outfile.write(str(new_infile))
    outfile.close()


def encode_mbl_item(root, root_ec, file, hl):
    infile = open(root + '/' + file, "r", encoding="utf-8").read()
    # infile = '"report_time": 1543909041000,'
    new_infile = infile

    # 手机号加密
    mb_reg = '1[3|4|5|7|8][0-9]{9}'
    mb_match = re.findall(mb_reg, new_infile)
    if len(mb_match) > 0 and not file.__contains__('td'):
        for mb in mb_match:
            hl.update(mb.encode(encoding='utf-8'))
            new_infile = new_infile.replace(mb, hl.hexdigest())

    # 保存加密后的数据
    outfile = open(root_ec + '/' + file, "w", encoding="utf-8")
    outfile.write(str(new_infile))
    outfile.close()


def encode_mbl_idno():
    hl = hashlib.md5()
    appnum = 0

    for root, dirs, files in os.walk(rootdir):
        print('app num', appnum)

        root_ec = root.replace("raw", "encryption")

        if len(files) > 0:
            folder_exists = os.path.exists(root_ec)
            if not folder_exists:
                os.makedirs(root_ec)

        for file in files:
            print(root_ec)
            encode_mbl_idno_item(root, root_ec, file, hl)

        appnum += 1

        # if appnum > 10:
        #     break


def encode_mbl():
    hl = hashlib.md5()
    appnum = 0

    for root, dirs, files in os.walk(rootdir):
        print('app num', appnum)

        root_ec = root.replace("raw", "encryption")

        if len(files) > 0:
            folder_exists = os.path.exists(root_ec)
            if not folder_exists:
                os.makedirs(root_ec)

        for file in files:
            print(root_ec)
            encode_mbl_item(root, root_ec, file, hl)

        appnum += 1

        # if appnum > 1:
        #     break


if __name__ == '__main__':
    encode_mbl()
