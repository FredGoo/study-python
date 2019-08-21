# -*- coding: utf-8 -*-
import json
import os

root_path = '/home/fred/git/study-python/etl/out/ningxia'
total_contact = []
common_contact = {}


def find_phone(contact_list):
    for phone in contact_list:
        if not common_contact.__contains__(phone):
            common_contact[phone] = []

        if total_contact[0].__contains__(phone):
            common_contact[phone].append('张娟')
        if total_contact[1].__contains__(phone):
            common_contact[phone].append('张娜')
        if total_contact[2].__contains__(phone):
            common_contact[phone].append('李雪')
        if total_contact[3].__contains__(phone):
            common_contact[phone].append('张芳')


for root, dirs, files in os.walk(root_path):
    for file in files:
        print(file)

        with open(root + '/' + file, 'r') as f:
            data = json.load(fp=f)
        contact_list = []
        for contact in data['report_data']['contact_list']:
            if not contact['phone_num'] in ('114', '10086', '10000', '10001'):
                contact_list.append(contact['phone_num'])
        total_contact.append(list(set(contact_list)))

# 0 1
retA = [i for i in total_contact[0] if i in total_contact[1]]
print("0 1 is: ", retA)
find_phone(retA)

# 0 2
retA = [i for i in total_contact[0] if i in total_contact[2]]
print("0 2 is: ", retA)
find_phone(retA)

# 0 3
retA = [i for i in total_contact[0] if i in total_contact[3]]
print("0 3 is: ", retA)
find_phone(retA)

# 1 2
retA = [i for i in total_contact[1] if i in total_contact[2]]
print("1 2 is: ", retA)
find_phone(retA)

# 1 3
retA = [i for i in total_contact[0] if i in total_contact[3]]
print("1 3 is: ", retA)
find_phone(retA)

# 2 3
retA = [i for i in total_contact[2] if i in total_contact[3]]
print("2 3 is: ", retA)
find_phone(retA)

for key in common_contact.keys():
    common_contact[key] = list(set(common_contact[key]))

print(common_contact)
