# -*- coding: utf8 -*-

import sqlite3
from os import path, makedirs

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

import common

CHUNKSIZE = 10 ** 5
HEADERS = ['user_id'
        , 'merchant_id'
        , 'action_0'
        , 'action_1'
        , 'action_2'
        , 'action_3'
        , 'age_range'
        , 'gender']
VERSION = 'v2'

def generate_features(conn):
    cur = conn.cursor()
    # create table
    cur.execute("""CREATE TABLE IF NOT EXISTS FEATURES_2 
            (user_id TEXT,
            merchant_id TEXT,
            action_0 TEXT,
            action_1 TEXT,
            action_2 TEXT,
            action_3 TEXT,
            age_range INT,
            gender INT);""")
    conn.commit()
    print "Finish create table"
    # construct feature 
    cur.execute("""INSERT INTO FEATURES_2 
            SELECT a.*, b.age_range, b.gender 
            FROM FEATURES_1 a, USERPROFILE b 
            WHERE a.user_id=b.user_id""")
    conn.commit()
    print "Finish insert data"

def generate_data_set(conn):
    p = './inter/%s' % VERSION
    if not path.exists(p):
        makedirs(p)
    
    common.construct_training_data_d(conn
            , VERSION
            , """SELECT a.*, b.label FROM FEATURES_2 a, TRAINING b 
            WHERE a.user_id = b.user_id and a.merchant_id = b.merchant_id"""
            , 8
            , HEADERS
            , CHUNKSIZE)
    common.construct_test_data_d(conn
            , VERSION
            , """SELECT a.* FROM FEATURES_2 a, TEST b 
            WHERE a.user_id = b.user_id and a.merchant_id = b.merchant_id"""
            , HEADERS
            , CHUNKSIZE)

def main():
    conn = sqlite3.connect("./inter/features.db")
    # generate_features(conn)
    generate_data_set(conn)
    conn.close()

if __name__ == "__main__":
    main()

