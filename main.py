# -*- coding: utf8 -*-

import sys
import logging

import pandas as pd
import numpy as np

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score

class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
    filename="out.log",
    filemode='w'
)

stderr_logger = logging.getLogger('STDERR')
sl = StreamToLogger(stderr_logger, logging.ERROR)
sys.stderr = sl

global v

def train():
    X_train = pd.read_csv('./inter/%s/feature_train_db.csv' % v)
    X_test = pd.read_csv('./inter/%s/feature_test_db.csv' % v)
    y_train = pd.read_csv('./inter/%s/label_train_db.csv' % v)
    y_test = pd.read_csv('./inter/%s/label_test_db.csv' % v)

    iter_num = 200
    lr = LogisticRegression(solver='sag', max_iter=iter_num, n_jobs=-1, warm_start=True)
        
    # 1. create lr model with different parameters
    # 2. train and test
    # 3. use the model with the best score
    print "\nStart train through LogisticRegression model..."
    print lr
    need = True
    batch = 0
    while need:
        print "*****************************************************************"
        print "Start batch %d" % batch
        lr.fit(X_train, y_train.values.ravel())
        print "Finish training with %s" % lr.n_iter_
        y_test_prob = lr.predict_proba(X_test)
        # print "Test score: %f" % brier_score_loss(y_test.values.ravel(), y_test_prob[:,1])
        print "Test score: %f" % roc_auc_score(y_test.values.ravel(), y_test_prob[:,1])
        batch += 1

        if lr.n_iter_[0] != iter_num:
            need = False
    
    return lr

def predict(model):
    print "\nStart generate the predicted result..."
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

