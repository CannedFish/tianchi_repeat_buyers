# -*- coding: utf8 -*-

import warnings

import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss
from sklearn.exceptions import ConvergenceWarning

global v

def train():
    X_train = pd.read_csv('./inter/%s/feature_train_db.csv' % v)
    X_test = pd.read_csv('./inter/%s/feature_test_db.csv' % v)
    y_train = pd.read_csv('./inter/%s/label_train_db.csv' % v)
    y_test = pd.read_csv('./inter/%s/label_test_db.csv' % v)

    lr = None
    with warnings.catch_warnings():
        warnings.filterwarnings('error')
        
        # 1. create lr model with different parameters
        # 2. train and test
        # 3. use the model with the best score
        need = True
        iter_num = 200
        while need:
            try:
                # TODO: try ues warm_start parameter
                lr = LogisticRegression(solver='sag', max_iter=iter_num, n_jobs=-1)
                print "Start train through LogisticRegression model..."
                print lr
                lr.fit(X_train, y_train.values.ravel())
            except ConvergenceWarning, w:
                print w
                iter_num += 200
            else:
                need = False
            # finally:
                # print "Finish training"
                # y_test_prob = lr.predict_proba(X_test)
                # print "Test score: %f" % brier_score_loss(y_test.values.ravel(), y_test_prob[:,1])

    print "Finish training"
    y_test_prob = lr.predict_proba(X_test)
    print "Test score: %f" % brier_score_loss(y_test.values.ravel(), y_test_prob[:,1])
    
    return lr

def predict(model):
    print "Start generate the predicted result..."
    X = pd.read_csv('./inter/%s/feature_predict_db.csv' % v)
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
    print "Data version is %s" % v
    predict(train())

if __name__ == '__main__':
    import sys
    global v

    if len(sys.argv) > 1:
        v = sys.argv[1]
    else:
        v = 'v1'

    main()

