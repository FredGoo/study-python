# -*- coding: utf-8 -*-
import csv

import numpy as np
import pydotplus
from sklearn import tree
from sklearn.model_selection import train_test_split
from tpot import TPOTClassifier


def start():
    with open('/home/fred/Documents/rmd/antifraud/out/app.csv') as f:
        data = list(csv.reader(f))
        del data[0]

    with open('/home/fred/Documents/rmd/antifraud/out/app_result.csv') as f:
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
    # pipeline_optimizer.fit(X_train, y_train)
    # print(pipeline_optimizer.score(X_test, y_test))
    # pipeline_optimizer.export('tpot_exported_pipeline.py')

    exported_pipeline = tree.DecisionTreeClassifier(criterion="gini", max_depth=7, min_samples_leaf=11,
                                                    min_samples_split=12)
    exported_pipeline.fit(X_train, y_train)
    tree.export_graphviz(exported_pipeline, out_file='tree.dot')
    results = exported_pipeline.predict(X_test)
    print(results)

    dot_data = tree.export_graphviz(exported_pipeline, out_file=None)
    graph = pydotplus.graph_from_dot_data(dot_data)
    graph.write_jpg("tree.jpg")  # 生成jpg文件


if __name__ == '__main__':
    start()
