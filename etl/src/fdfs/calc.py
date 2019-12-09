# -*- coding: utf-8 -*-

res = {}
with open('/tmp/789') as f:
    line = f.readline()
    while line:
        line = line.lstrip().rstrip('\n')
        agrs = line.split(' ')
        print(agrs)

        filesize = agrs[0]
        pfix = agrs[len(agrs) - 1].split('.')[2]

        if not res.__contains__(pfix):
            res[pfix] = 0
        res[pfix] += int(filesize)

        # break
        line = f.readline()

for reskey in res.keys():
    res[reskey] = str(res[reskey] / 1024 / 1024 / 1024) + 'G'

print(res)
