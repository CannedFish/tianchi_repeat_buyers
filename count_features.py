# -*- coding: utf8 -*-

import pandas as pd
import numpy as np

import sqlite3

CHUNKSIZE = 10 ** 5
MONTH = ['%02d' % x for x in xrange(5, 12)]
KEYs = ['user_id', 'item_id', 'cat_id', 'merchant_id', 'brand_id']

def action_count(monthly=True):
    def __insert_data(_data_cache, _cols, __dtype):
        df = pd.DataFrame(_data_cache, columns=_cols)
        df.astype(__dtype)
        df['total_action'] = df['action_0'] + df['action_2'] + df['action_3']
        df['action_0_ratio'] = df['action_0'] / df['total_action']
        df['action_2_ratio'] = df['action_2'] / df['total_action']
        df['action_3_ratio'] = df['action_3'] / df['total_action']
        df.drop(['total_action'], axis=1).to_csv(p, index=False, header=False, mode='a')
        print "Inserted %d records" % len(_data_cache)

    conn = sqlite3.connect("./inter/features.db")
    features = ['action_0', 'action_2', 'action_3', 'action_0_ratio', 'action_2_ratio', 'action_3_ratio']
    for key in KEYs:
        print "\nStart handle %s's %s action count" % (key, 'monthly' if monthly else 'overall')
        cols = [key]
        if monthly:
            cols.append('time_stamp')
        cols.extend(features)
        
        _dtype = {
            key: str, 
            'action_0': np.int32,
            'action_2': np.int32,
            'action_3': np.int32
        }
        if monthly:
            _dtype['time_stamp'] = np.int32
        p = "./inter/v3/%s_monthly_action.csv" % key if monthly else "./inter/v3/%s_overall_action.csv" % key
        pd.DataFrame(columns= cols).to_csv(p, index=False)

        print "Making aggragation..."
        batch = 0
        rec_num = 0
        data_cache = []
        _header = cols[:5] if monthly else cols[:4]
        if monthly:
            sql = """SELECT %s, time_stamp, sum(action_0), sum(action_2), sum(action_3) 
                FROM USERLOG GROUP BY %s, time_stamp""" % (key, key)
        else:
            sql = """SELECT %s, sum(action_0), sum(action_2), sum(action_3) 
                FROM USERLOG GROUP BY %s""" % (key, key)
        for rec in conn.execute(sql):
            data_cache.append(rec)
            rec_num += 1

            if rec_num == CHUNKSIZE:
                print "Batch %d:" % batch
                __insert_data(data_cache, _header, _dtype)

                rec_num = 0
                data_cache = []
                batch += 1

        if rec_num > 0:
            __insert_data(data_cache, _header, _dtype)

        print "Totally completed with %s" % key

def basic_data_transform():
    """
    Transform "action_type" into the number of "action_0", "action_2", "action_3"
    """
    def __monthly_record(row, _df):
        _df.at[row.name, 'action_%d' % row['action_type']] = 1

    conn = sqlite3.connect("./inter/features.db")
    batch = 0
    print "Reading & Reshaping data..."
    for df in pd.read_csv("./data/data_format1/user_log_format1.csv"
            , chunksize=CHUNKSIZE
            , dtype={'user_id': str, 
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str, 
                'action_type': np.int32}):
        # Map month to number
        df['time_stamp'] = df['time_stamp'].apply(lambda x: MONTH.index(x[:2]))
        df.apply(__monthly_record, axis=1, args=(df,))
        df = df.fillna(0)
        df['action_0'] += df['action_1']
        df.drop(['action_type', 'action_1'], axis=1)\
                .to_sql('USERLOG', conn, index=False, if_exists='append')
        print "Batch %d completed" % batch
        batch += 1

    print "Totally completed"
    conn.close()

    # NOTE: Python memory model is wield
    # print "Aggragating data..."
    # aggr_df = pd.concat(dfs)
    # common_label = ['time_stamp', 'action_0', 'action_2', 'action_3']
    # for key in KEYs:
        # print "\nGenerate monthly count with %s" % key
        # target_cols = [key]
        # target_cols.extend(common_label)
        # sub_df = aggr_df.select(lambda l: l in target_cols, axis=1)\
                # .groupby([key, 'time_stamp'])\
                # .sum()\
                # .to_csv('./inter/v3/%s_monthly_action.csv' % key)
        # # sub_df.reset_index(level=[key, 'time_stamp'], inplace=True)
        # # sub_df.to_csv('./inter/v3/%s_monthly_action.csv' % key, index=False)
        # print "Generate monthly count with %s completed." % key

def main():
    # basic_data_transform()
    action_count(False)

if __name__ == '__main__':
    main()

