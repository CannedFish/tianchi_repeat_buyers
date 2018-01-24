# -*- coding: utf8 -*-

import pandas as pd
import numpy as np

from graph_db import db

CHUNKSIZE = 10 ** 3

def create_user_nodes():
    for df in pd.read_csv("./data/data_format1/user_info_format1.csv"
            , chunksize=CHUNKSIZE):
        df = df.fillna(-1).astype({'user_id': str, 'age_range': np.int32, 'gender': np.int32})
        nodes = [{
            'label': 'User',
            'prop': {
                'user_id': row['user_id'],
                'age_range': row['age_range'],
                'gender': row['gender']
            }} for idx, row in df.iterrows()]
        db.create_nodes(nodes)
    print "Create user nodes finished."

def create_merchant_nodes():
    merchants = []
    for df in pd.read_csv("./data/data_format1/user_log_format1.csv"
            , chunksize=CHUNKSIZE
            , dtype={
                'user_id': str,
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str,
                'action_tpye': np.int32
            }):
        merchants.extend(pd.unique(df['merchant_id']).tolist())
        merchants = list(set(merchants))
        print "%d element handled, %d merchants total now." % (df.size, len(merchants))
    nodes = [{
        'label': 'Merchant',
        'prop': {
            'merchant_id': merchant
        }} for merchant in merchants]
    db.create_nodes(nodes)

def create_activity_nodes(df):
    nodes = [{
        'label': 'Activity',
        'prop': {
            'activity_id': row['activity_id'],
            'item_id': row['item_id'],
            'cat_id': row['cat_id'],
            'brand_id': row['brand_id'],
            'time_stamp': row['time_stamp'],
            'action_tpye': row['action_type']
        }} for idx, row in df.iterrows()]
    db.create_nodes(nodes)
    return len(nodes)

def create_relationships():
    nodes_num = 0
    for df in pd.read_csv("./data/data_format1/user_log_format1.csv"
            , chunksize=CHUNKSIZE
            , dtype={
                'user_id': str,
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str,
                'action_tpye': np.int32
            }):
        # 1. extract activity node info
        # 2. format :DO and :TO relations
        print "Start building activity nodes batch."
        df = df.fillna('null')
        df['activity_id'] = df['user_id'] + df['item_id'] + df['cat_id'] + \
                df['merchant_id'] + df['brand_id'] + df['time_stamp'] + \
                df['action_type'].astype(str)
        nodes_num += create_activity_nodes(df)
        print "%d activity nodes are built." % nodes_num
        
        # for idx, row in df.iterrows():
            # db.create_edge(('user_id', row['user_id'])
                    # , ('activity_id', row['activity_id'])
                    # , 'DO')
            # db.create_edge(('activity_id', row['activity_id'])
                    # , ('merchant_id', row['merchant_id'])
                    # , 'TO')

def main():
    # create_user_nodes()
    # create_merchant_nodes()
    create_relationships()

if __name__ == '__main__':
    main()

