import pandas as pd
import numpy as np

import json
from graph_db import db

"""
USER = {
    '@id': '',
    '@type': 'User',
    'age_range': '',
    'gender': '',
    'merchants': {
}
"""

CHUNKSIZE = 10 ** 4

def save_users(users):
    pass

def get_user(user_id):
    """
    user = {
        'user_id': '',
        'age_range': '',
        'gender': '',
        'merchants': {
            '$id': {
                'label': '',
                'activity_log': []
            },
            ...
        },
    }
    """
    pass

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
    pass

def create_activity_nodes():
    pass

def create_relationships():
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
        for idx, row in df.iterrows():
            # TODO:
            # 1. extract merchant node info
            # 2. extract activity node info
            # 3. format :DO and :TO relations
            pass

def main():
    # create_user_nodes()
    create_relationships()

if __name__ == '__main__':
    main()

