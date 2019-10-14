# -*- coding: utf-8 -*-
import json
import os
import shutil

source_root_dir = '/home/fred/Documents/2.rmd/2.kezhi/sample20190926'
rootdir = '/home/fred/Documents/2.rmd/2.kezhi/sample20190927'


def check_json():
    file_path = rootdir + '/encryption'
    i = 0
    for root, dirs, files in os.walk(file_path):

        for file in files:
            try:
                with open(root + '/' + file) as f:
                    content = f.read()
                    # if content == '':
                    #     os.remove(root + '/' + file)
                    # else:
                    json.loads(content)

            except:
                # if file.startswith("zq"):
                #     shutil.copy2(source_root_dir + '/raw/' + root.split("/")[8] + '/' + file, root + '/' + file)
                # else:
                    # print('json.load error', sys.exc_info()[0])
                print(root, file)
                    # return

        # print('deal num', i)
        i += 1
        # if i > 2:
        #     break


if __name__ == '__main__':
    check_json()
