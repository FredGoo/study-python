from tpot import TPOTClassifier


def start():
    pipeline_optimizer = TPOTClassifier(generations=5, population_size=20, cv=5,
                                        random_state=42, verbosity=2)

    pipeline_optimizer.fit('x_train', 'y_train')


if __name__ == '__main__':
    start()
