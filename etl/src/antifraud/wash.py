# -*- coding: utf-8 -*-
import json

field_map = {
    'br_sbyz': {}
}


# 百融
def br_sbyz(path):
    with open(path, 'r') as f:
        data = json.load(fp=f)

    print(data)


# 天行

# 凭安

# 同盾

# 魔蝎

# 集奥

# 新颜

if __name__ == '__main__':
    br_sbyz('/home/fred/git/study-python/etl/out/raw/NYB01-181204-200610/br_BRSBYZ.json')
