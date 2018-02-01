# -*- coding: utf8 -*-

import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

def __generate_feature_file_header(p, HEADERS):
    pd.DataFrame(columns=HEADERS).to_csv(p, index=False)

def __generate_label_file_header(p):
    pd.DataFrame(columns=['label']).to_csv(p, index=False)

def __insert_feature_file(n_data, p, HEADERS):
    pd.DataFrame(data=n_data, columns=HEADERS).to_csv(p, mode='a', header=False, index=False)

def __insert_label_file(n_data, p):
    pd.DataFrame(data=n_data, columns=['label']).to_csv(p, mode='a', header=False, index=False)

def construct_training_data_d(conn, version, sql, label_pos, hdrs, chks):
    """
    Based on sqlite3
    """
    f_tr_p = './inter/%s/feature_train_db.csv' % version
    f_te_p = './inter/%s/feature_test_db.csv' % version
    l_tr_p = './inter/%s/label_train_db.csv' % version
    l_te_p = './inter/%s/label_test_db.csv' % version
    
    def __insert_data(X, y):
        print "Start insert data..."
        X_train, X_test, y_train, y_test = train_test_split(X, y)

        __insert_feature_file(X_train, f_tr_p, hdrs)
        __insert_feature_file(X_test, f_te_p, hdrs)
        __insert_label_file(y_train, l_tr_p)
        __insert_label_file(y_test, l_te_p)
        print "Finish insert data, with %d records." % len(X)

    __generate_feature_file_header(f_tr_p, hdrs)
    __generate_feature_file_header(f_te_p, hdrs)
    __generate_label_file_header(l_tr_p)
    __generate_label_file_header(l_te_p)
    cur = conn.cursor()
    rec_num = 0
    X, y = [], []
    print "Start construct training & test data set based on sqlite3..."
    for row in cur.execute(sql):
        X.append(row[:label_pos])
        y.append(row[label_pos:])
        rec_num += 1

        if rec_num == chks:
            __insert_data(X, y)
            
            rec_num = 0
            X, y = [], []
    
    if rec_num != 0:
        __insert_data(X, y)
    
    print "Finish construct training & test data set"

def construct_test_data_d(conn, version, sql, hdrs, chks):
    """
    Based on sqlite3
    """
    f_pr_p = './inter/%s/feature_predict_db.csv' % version
    __generate_feature_file_header(f_pr_p, hdrs)
    cur = conn.cursor()
    rec_num = 0
    X = []
    print "Start construct test data set for prediction based on sqlite3..."
    for row in cur.execute(sql):
        X.append(row)
        rec_num += 1

        if rec_num == chks:
            print "Start insert data..."
            __insert_feature_file(X, f_pr_p, hdrs)
            print "Finish insert data, with %d records." % len(X)

            rec_num = 0
            X = []
 
    if rec_num != 0:
        print "Start insert data..."
        __insert_feature_file(X, f_pr_p, hdrs)
        print "Finish insert data, with %d records." % len(X)
    
    print "Finish construct test data set"
 
