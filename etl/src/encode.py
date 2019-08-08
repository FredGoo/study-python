# encoding:utf-8

import hashlib
import os
import re


def encode_mbl_idno_item(root, root_ec, file, hl):
    infile = open(root + '/' + file, "r", encoding="utf-8").read()
    new_infile = infile

    # 手机号加密
    mb_reg = '1[3|4|5|7|8][0-9]{9}'
    mb_match = re.findall(mb_reg, new_infile)
    if len(mb_match) > 0:
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

    print(new_infile)

    # 保存加密后的数据
    outfile = open(root_ec + '/' + file, "w", encoding="utf-8")
    outfile.write(str(new_infile))
    outfile.close()


def encode_mbl_idno():
    rootdir = os.getcwd()[:-4] + '/out/raw'
    hl = hashlib.md5()

    for root, dirs, files in os.walk(rootdir):
        root_ec = root.replace("raw", "encryption")

        if len(files) > 0:
            folder_exists = os.path.exists(root_ec)
            if not folder_exists:
                os.makedirs(root_ec)

        for file in files:
            encode_mbl_idno_item(root, root_ec, file, hl)


if __name__ == '__main__':
    encode_mbl_idno()
