# -*- coding: utf8 -*-

# TODO: 
# 1. convert data in user_log into User-Merchant featuers which appear in training_data;
# 2. split converted data into training data and validation data;
import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

CHUNKSIZE = 10 ** 5

def generate_features():
    conn = sqlite3.connect('./data/data.db')
    cur = conn.cursor()
    pd.DataFrame(columns=['user_id', 'merchant_id', 'action_0', 'action_1', 'action_2', 'action_3', 'label']).to_csv('./inter/features.csv', index=False)
    for user in cur.execute("select distinct user_id from training").fetchall():
        print "Start handle records with user(%s)" % (user)
        user_id = []
        merchant_id = []
        action_0 = []
        action_1 = []
        action_2 = []
        action_3 = []
        label = []
        for rec in cur.execute("select * from training where user_id='%s'" % user[0]).fetchall():
            user_id.append(rec[0])
            merchant_id.append(rec[1])
            label.append(rec[2])
            actions = [0, 0, 0, 0]
            for log in cur.execute("select action_type from userlog where user_id=(?) and merchant_id=(?)"
                    , (rec[0], rec[1])):
                actions[log[0]] += 1
            action_0.append(actions[0])
            action_1.append(actions[1])
            action_2.append(actions[2])
            action_3.append(actions[3])
        pd.DataFrame(data={
            'user_id': user_id,
            'merchant_id': merchant_id,
            'action_0': action_0,
            'action_1': action_1,
            'action_2': action_2,
            'action_3': action_3,
            'label': label
        }, columns=['user_id'
            , 'merchant_id'
            , 'action_0'
            , 'action_1'
            , 'action_2'
            , 'action_3'
            , 'label']).to_csv('./inter/features.csv', mode='a', header=False, index=False)
        print "Finish insert %d records with user(%s)" % (len(user_id), user)

def __generate_feature_file_header(p):
    pd.DataFrame(columns=['user_id'
        , 'merchant_id'
        , 'action_0'
        , 'action_1'
        , 'action_2'
        , 'action_3']
        ).to_csv(p, index=False)

def __generate_label_file_header(p):
    pd.DataFrame(columns=['label']).to_csv(p, index=False)

def __insert_feature_file(n_data, p):
    pd.DataFrame(data=n_data, columns=['user_id'
        , 'merchant_id'
        , 'action_0'
        , 'action_1'
        , 'action_2'
        , 'action_3']).to_csv(p, mode='a', header=False, index=False)

def __insert_label_file(n_data, p):
    pd.DataFrame(data=n_data, columns=['label']).to_csv(p, mode='a', header=False, index=False)

def generate_train_test():
    print "Start split data into train and test set..."
    __generate_feature_file_header('./inter/feature_train.csv')
    __generate_feature_file_header('./inter/feature_test.csv')
    __generate_label_file_header('./inter/label_train.csv')
    __generate_label_file_header('./inter/label_test.csv')
    batch = 0
    for df in pd.read_csv('./inter/features.csv'
            , chunksize=CHUNKSIZE, dtype={
            'user_id': str,
            'merchant_id': str,
            'action_0': np.int32,
            'action_1': np.int32,
            'action_2': np.int32,
            'action_3': np.int32,
            'label': np.int32}):
        print "Batch %d" % batch
        
        X_train, X_test, y_train, y_test = train_test_split(df[['user_id'
            , 'merchant_id'
            , 'action_0'
            , 'action_1'
            , 'action_2'
            , 'action_3']], df['label'])
        __insert_feature_file(X_train, './inter/feature_train.csv')
        __insert_feature_file(X_test, './inter/feature_test.csv')
        __insert_label_file(y_train, './inter/label_train.csv')
        __insert_label_file(y_test, './inter/label_test.csv')
        
        batch += 1
    
    print "Finish split data into train and test set..."

def generate_features_directly():
    conn = sqlite3.connect('./inter/features.db')
    cur = conn.cursor()
    print "Start create temp feature table..."
    cur.execute("""CREATE TABLE IF NOT EXISTS FEATURES_1_TEMP 
            (user_id TEXT,
            merchant_id TEXT,
            action_0 TEXT,
            action_1 TEXT,
            action_2 TEXT,
            action_3 TEXT);""")
    conn.commit()
    print "Finish create temp feature table"

    # pd.DataFrame(columns=['user_id'
        # , 'merchant_id'
        # , 'action_0'
        # , 'action_1'
        # , 'action_2'
        # , 'action_3']
        # ).to_csv('./inter/inter_features1.csv', index=False)
    print "Start insert data into temp feature table..."
    for df in pd.read_csv('./data/data_format1/user_log_format1.csv',
            chunksize=CHUNKSIZE,
            dtype={
                'user_id': str,
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str,
                'action_type': np.int32
            }):
        user_id = []
        merchant_id = []
        action = [[], [], [], []]
        for g_idx, groupdf in df.groupby(['user_id', 'merchant_id']):
            actions = [0, 0, 0, 0]
            for idx, row in groupdf.iterrows():
                actions[row['action_type']] += 1
            user_id.append(g_idx[0])
            merchant_id.append(g_idx[1])
            for _ in xrange(4):
                action[_].append(actions[_])
        # TODO: use sqlite to perform merge action
        # 1. cache data into a temp table
        # 2. save merged result into feature_1
        params = [(u, m, a0, a1, a2, a3) \
                for u, m, a0, a1, a2, a3 \
                in zip(user_id, merchant_id, action[0], action[1], action[2], action[3])]
        cur.executemany("INSERT INTO FEATURES_1_TEMP VALUES (?,?,?,?,?,?)", params)
        conn.commit()
        print "Inserted %d data into temp feature table" % len(params)
    print "Finish insert data into temp feature table"

    print "Start insert data into feature table..."
    cur.execute("INSERT INTO FEATURES_1 SELECT user_id, merchant_id, SUM(action_0), SUM(action_1), SUM(action_2), SUM(action_3) FROM FEATURES_1_TEMP GROUP BY user_id, merchant_id")
    conn.commit()
    print "Finish insert data into feature table"

        # pd.DataFrame(data={
            # 'user_id': user_id,
            # 'merchant_id': merchant_id,
            # 'action_0': action[0],
            # 'action_1': action[1],
            # 'action_2': action[2],
            # 'action_3': action[3],
        # }, columns=['user_id'
            # , 'merchant_id'
            # , 'action_0'
            # , 'action_1'
            # , 'action_2'
            # , 'action_3']).to_csv('./inter/inter_features1.csv'
                    # , mode='a', header=False, index=False)

def main():
    pass

if __name__ == '__main__':
    import sys

    if sys.argv[1] == 'd':
        print "genarate features directly"
        generate_features_directly()
    else:
        print "genarate features"
        generate_features()

