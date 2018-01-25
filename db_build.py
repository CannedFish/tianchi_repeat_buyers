# -*- coding: utf8 -*-

import sqlite3
import pandas as pd
import numpy as np

def build_tables(conn):
    print "Start building tables..."
    cur = conn.cursor()
    # USERLOG
    cur.execute("""CREATE TABLE IF NOT EXISTS USERLOG 
            (user_id TEXT,
            item_id TEXT,
            cat_id TEXT,
            merchant_id TEXT,
            brand_id TEXT,
            time_stamp TEXT,
            action_type INT);""")
    # USERPROFILE
    cur.execute("""CREATE TABLE IF NOT EXISTS USERPROFILE 
            (user_id TEXT,
            age_range INT,
            gender INT);""")
    # TRAINING
    cur.execute("""CREATE TABLE IF NOT EXISTS TRAINING 
            (user_id TEXT,
            merchant_id TEXT,
            label INT);""")
    # FEATURES_1
    cur.execute("""CREATE TABLE IF NOT EXISTS FEATURES_1 
            (user_id TEXT,
            merchant_id TEXT,
            action_0 TEXT,
            action_1 TEXT,
            action_2 TEXT,
            action_3 TEXT);""")

    conn.commit()
    print "Finish building tables..."

CHUNKSIZE = 10 ** 4

def import_data(conn):
    print "Start import data..."
    # user log
    for df in pd.read_csv("./data/data_format1/user_log_format1.csv"
            , chunksize=CHUNKSIZE
            , dtype={
                'user_id': str,
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str,
                'action_type': np.int32
            }):
        df = df.fillna('null')
        params = [(row['user_id'], row['item_id'], row['cat_id'], row['merchant_id'], row['brand_id'], row['time_stamp'], row['action_type']) for idx, row in df.iterrows()]
        cur = conn.cursor()
        cur.executemany("INSERT INTO USERLOG VALUES (?,?,?,?,?,?,?)", params)
        conn.commit()
        print "%d user logs imported." % (df.size/7)

    # user profile
    for df in pd.read_csv("./data/data_format1/user_info_format1.csv"
            , chunksize=CHUNKSIZE):
        df = df.fillna(-1).astype({'user_id': str, 'age_range': np.int32, 'gender': np.int32})
        params = [(row['user_id'], row['age_range'], row['gender']) for idx, row in df.iterrows()]
        cur = conn.cursor()
        cur.executemany("INSERT INTO USERPROFILE VALUES (?,?,?)", params)
        conn.commit()
        print "%d user profiles imported." % (df.size/3)

    # train
    for df in pd.read_csv("./data/data_format1/train_format1.csv"
            , chunksize=CHUNKSIZE):
        df = df.fillna(-1).astype({'user_id': str, 'merchant_id': str, 'label': np.int32})
        params = [(row['user_id'], row['merchant_id'], row['label']) for idx, row in df.iterrows()]
        cur = conn.cursor()
        cur.executemany("INSERT INTO TRAINING VALUES (?,?,?)", params)
        conn.commit()
        print "%d training data imported." % (df.size/3)
    print "Finish import data..."

def exam(conn):
    print "Start exam data..."
    cur = conn.cursor()
    
    print "User Log:"
    print cur.execute("SELECT count(*) FROM USERLOG").fetchone()

    print "User Profile:"
    print cur.execute("SELECT count(*) FROM USERPROFILE").fetchone()

    print "Train:"
    print cur.execute("SELECT count(*) FROM TRAINING").fetchone()
    print "Finish exam data..."

def main():
    conn = sqlite3.connect('./data/data.db')

    build_tables(conn)
    import_data(conn)
    exam(conn)

    conn.close()

if __name__ == '__main__':
    main()

