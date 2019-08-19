# -*- coding: utf-8 -*-
import base64
import hashlib
import json

import requests
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.PublicKey import RSA

db_config = {}
api_url = 'https://smarteye.unionpaysmart.com/index/merchant'

if __name__ == '__main__':
    # 获取配置
    with open('config/unionpay.json', 'r') as f:
        config = json.load(fp=f)

    data = {"mid": "876543212345678"}

    # 加密参数
    key1 = base64.b64decode(config['privateKeyGeex'])
    # key1 = base64.b64decode(config['privateKeyUnion'])
    rsakey = RSA.importKey(key1)
    cipher = Cipher_pkcs1_v1_5.new(rsakey)
    temp = cipher.encrypt(json.dumps(data).encode(encoding="utf-8"))
    cipher_text = base64.b64encode(temp)

    # 签名
    hl = hashlib.md5()
    sign_content = config['account'] + str(cipher_text, encoding="utf-8")
    hl.update(sign_content.encode(encoding='utf-8'))
    sign = hl.hexdigest()

    # 数据体
    request_body = {
        "account": config['account'],
        "encrypt": str(cipher_text, encoding="utf-8"),
        "sign": sign
    }
    print(request_body)

    # 请求结果
    r = requests.post(api_url, data=request_body)
    print(str(r.content, 'utf-8'))
