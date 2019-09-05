# -*- coding: utf-8 -*-

# 奇怪号码过滤
def oddphone_filter(mobile):
    if len(mobile) < 6:
        return True
    elif mobile.startswith('400') or mobile.startswith('40*') or mobile.startswith('800'):
        return True
    elif len(mobile) == 10:
        if mobile.startswith('+86'):
            return True
