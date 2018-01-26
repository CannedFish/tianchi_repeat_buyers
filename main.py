# -*- coding: utf8 -*-

import pandas as pd
from sklearn.linear_model import LogisticRegression

def train():
    X_train = pd.read_csv('./inter/feature_train.csv')
    X_test = pd.read_csv('./inter/feature_test.csv')
    y_train = pd.read_csv('./inter/label_train.csv')
    y_test = pd.read_csv('./inter/label_test.csv')
    while False:
        # TODO:
        # 1. create lr model with different parameters
        # 2. train and test
        # 3. use the model with the best score
        lr = LogisticRegression(solver='sag')
    pass

def main():
    pass

if __name__ == '__main__':
    main()

