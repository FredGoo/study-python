# -*- coding: utf-8 -*-
import csv

import numpy as np
from sklearn.model_selection import train_test_split
from tpot import TPOTClassifier


def start():
    with open('/home/fred/Documents/2.rmd/1.antifraud/out/app.csv') as f:
        data = list(csv.reader(f))
        del data[0]

    with open('/home/fred/Documents/2.rmd/1.antifraud/out/app_result.csv') as f:
        target = list(csv.reader(f))
        del target[0]

    pipeline_optimizer = TPOTClassifier(generations=10, population_size=20, cv=5,
                                        random_state=42, verbosity=2)

    data_np = np.array(data)
    data_np1 = data_np.astype(np.float)
    target_np = np.array(target)
    target_np1 = target_np.astype(np.float)

    X_train, X_test, y_train, y_test = train_test_split(data_np1, target_np1,
                                                        train_size=0.75, test_size=0.25)
    pipeline_optimizer.fit(X_train, y_train)
    print(pipeline_optimizer.score(X_test, y_test))
    pipeline_optimizer.export('tpot_exported_pipeline.py')


if __name__ == '__main__':
    start()
