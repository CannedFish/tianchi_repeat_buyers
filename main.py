# -*- coding: utf8 -*-

import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression

def train():
    X_train = pd.read_csv('./inter/feature_train_db.csv')
    X_test = pd.read_csv('./inter/feature_test_db.csv')
    y_train = pd.read_csv('./inter/label_train_db.csv')
    y_test = pd.read_csv('./inter/label_test_db.csv')
    
    lr = LogisticRegression(solver='sag')
    while True:
        # TODO:
        # 1. create lr model with different parameters
        # 2. train and test
        # 3. use the model with the best score
        print "Start train through LogisticRegression model..."
        lr.fit(X_train, y_train.values.ravel())
        print "Finish training"
        print "Test score: %f" % lr.score(X_test, y_test.values.ravel())
        break

    return lr

def predict(model):
    print "Start generate the predicted result..."
    X = pd.read_csv('./inter/feature_predict_db.csv')
    y_pred = model.predict_proba(X)

    cols = ['user_id', 'merchant_id']
    # index = np.array([X['user_id'].values, X['merchant_id'].values]).transpose()
    df_pred = pd.DataFrame(data={
        'user_id': X['user_id'],
        'merchant_id': X['merchant_id'],
        'prob': y_pred[:,1]}
        # , index=index
        , columns=['user_id', 'merchant_id', 'prob'])
    pd.read_csv('./data/data_format1/test_format1.csv', usecols=cols)\
            .join(df_pred.set_index(cols), on=cols)\
            .to_csv('./out/submission.csv', index=False)
    print "Finish generate the predicted result..."

def main():
    predict(train())

if __name__ == '__main__':
    main()

