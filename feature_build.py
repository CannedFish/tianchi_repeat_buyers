# -*- coding: utf8 -*-

# TODO: 
# 1. convert data in user_log into User-Merchant featuers which appear in training_data;
# 2. split converted data into training data and validation data;
import sqlite3
import pandas as pd
import numpy as np

CHUNKSIZE = 10 ** 5

def generate_features():
    conn = sqlite3.connect('./data/data.db')
    cur = conn.cursor()
    pd.DataFrame(columns=['user_id', 'merchant_id', 'action_0', 'action_1', 'action_2', 'action_3', 'label']).to_csv('./inter/features.csv')
    for user in cur.execute("select distinct user_id from training").fetchall():
        user_id = []
        merchant_id = []
        action_0 = []
        action_1 = []
        action_2 = []
        action_3 = []
        label = []
        actions = [0, 0, 0, 0]
        for rec in cur.execute("select * from training where user_id=(?)", (user,)).fetchall():
            user_id.append(rec[0])
            merchant_id.append(rec[1])
            label.append(rec[2])
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
        }).to_csv('./inter/features.csv', mode='a', header=False)

def generate_features_directly():
    # pd.DataFrame(columns=['user_id'
        # , 'merchant_id'
        # , 'action_0'
        # , 'action_1'
        # , 'action_2'
        # , 'action_3']
        # ).to_csv('./inter/inter_features1.csv', index=False)
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
        # 1. cache data with :memory:
        # 2. save merged result into feature_1

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
    main()

