# encoding:utf-8

from tpot import TPOTClassifier


def start():
    pipeline_optimizer = TPOTClassifier(generations=5, population_size=20, cv=5,
                                        random_state=42, verbosity=2)

    pipeline_optimizer.fit([['x']], ['y'])


if __name__ == '__main__':
    start()
