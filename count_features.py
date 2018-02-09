# -*- coding: utf8 -*-

import pandas as pd
import numpy as np

import sqlite3

CHUNKSIZE = 10 ** 5
MONTH = ['%02d' % x for x in xrange(5, 12)]
KEYs = ['user_id', 'item_id', 'cat_id', 'merchant_id', 'brand_id']

# Action count
def action_count(monthly=True):
    """
    Overall and monthly day count and ratio for user, item, brand, category, merchant
    """
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
            sql = """SELECT %s, month, sum(action_0), sum(action_2), sum(action_3) 
                FROM USERLOG GROUP BY %s, month""" % (key, key)
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

# Day counts
def day_count(monthly=True):
    """
    Overall and monthly day count for user, item, brand, category, merchant
    """
    def __insert_data(_data_cache, _cols):
        pd.DataFrame(_data_cache, columns=_cols).to_csv(p, index=False, header=False, mode='a')
        print "Inserted %d records" % len(_data_cache)

    conn = sqlite3.connect("./inter/features.db")
    for key in KEYs:
        print "\nStart handle %s's %s day count" % (key, 'monthly' if monthly else 'overall')
        p = "./inter/v3/%s_monthly_day.csv" % key if monthly \
                else "./inter/v3/%s_overall_day.csv" % key
        features = [key, 'month'] if monthly else [key]
        features.extend(['action_type', 'day_count'])
        pd.DataFrame(columns=features).to_csv(p, index=False)

        print "Making aggragation..."
        batch = 0
        rec_num = 0
        data_cache = []
        sql = "SELECT %s, month, action_type, count(distinct(time_stamp)) \
                FROM USERLOG GROUP BY %s, month, action_type" % (key, key) if monthly \
                else "SELECT %s, action_type, count(distinct(time_stamp)) \
                FROM USERLOG GROUP BY %s, action_type" % (key, key)
        for rec in conn.execute(sql):
            data_cache.append(rec)
            rec_num += 1

            if rec_num == CHUNKSIZE:
                print "Batch %d" % batch
                __insert_data(data_cache, features)
                
                rec_num = 0
                data_cache = []
                batch += 1

        if rec_num > 0:
            print "Batch %d" % batch
            __insert_data(data_cache, features)

        print "Totally completed with %s" % key

def __insert_action_data(_data_cache, _to, _cols, _dtype):
    df = pd.DataFrame(_data_cache, columns=_cols[:-3])
    df.astype(_dtype)
    df['total_action'] = df['action_0'] + df['action_2'] + df['action_3']
    df['action_0_ratio'] = df['action_0'] / df['total_action']
    df['action_2_ratio'] = df['action_2'] / df['total_action']
    df['action_3_ratio'] = df['action_3'] / df['total_action']
    df.drop(['total_action'], axis=1).to_csv(_to, index=False, header=False, mode='a')
    print "Inserted %d records" % len(_data_cache)

def __insert_day_data(_data_cache, _to, _cols, _dt):
    pd.DataFrame(_data_cache, columns=_cols).to_csv(_to, index=False, header=False, mode='a')
    print "Inserted %d records" % len(_data_cache)

def __sql(conn, sql, to, batchsize, batch_handler, *args):
    print "Making aggragation..."
    batch = 0
    rec_num = 0
    data_cache = []
    for rec in conn.execute(sql):
        data_cache.append(rec)
        rec_num += 1

        if rec_num == batchsize:
            print "Batch %d" % batch
            batch_handler(data_cache, to, *args)

            rec_num = 0
            data_cache = []
            batch += 1

    if rec_num > 0:
        print "Batch %d" % batch
        batch_handler(data_cache, to, *args)

PAIRs = [
    ('user_id', 'merchant_id'),
    ('user_id', 'brand_id'),
    ('user_id', 'cat_id'),
    ('merchant_id', 'brand_id'),
    ('merchant_id', 'cat_id')
]

# Pair counts
def pair_count():
    """
    Overall action count and day count for user-merchant, user-brand, user-category, 
    merchant-brand and merchant-category
    """
    print "Pair conut"
    count_type = [
        (
            'action',
            'SELECT %s, %s, sum(action_0), sum(action_2), sum(action_3) FROM USERLOG GROUP BY %s, %s',
            ['action_0', 'action_2', 'action_3', 'action_0_ratio', 'action_2_ratio', 'action_3_ratio'],
            {'action_0': np.int32, 'action_2': np.int32, 'action_3': np.int32},
            __insert_action_data,
            './inter/v3/count/%s/%s_%s_overall_%s.csv'
        ),
        # (
            # 'day',
            # 'SELECT %s, %s, month, action_type, count(distinct(time_stamp)) FROM USERLOG GROUP BY %s, %s, month, action_type',
            # ['month', 'action_type', 'day_count'],
            # {},
            # __insert_day_data,
            # './inter/v3/count/%s/%s_%s_monthly_%s.csv'
        # )
    ]
    conn = sqlite3.connect('./inter/features.db')

    for pair in PAIRs:
        for ct, sqlfmt, cf, dt, handler, targetfmt in count_type:
            print "\nStart handle %s-%s pair's %s count" % (pair[0], pair[1], ct)
            features = list(pair)
            features.extend(cf)
            p = targetfmt % (ct, pair[0], pair[1], ct)
            pd.DataFrame(columns=features).to_csv(p, index=False)
            __sql(conn, sqlfmt % (pair[0], pair[1], pair[0], pair[1]), p,
                    CHUNKSIZE, handler, features, dt)
            print "Totally completed with %s_%s pair" % pair

    conn.close()

# Product diversity
def product_diversity():
    """
    The number of unique items, brands and categories that user, merchant or user-merchant pair 
    clicked, added to favourites or purchased monthly or overall.
    """
    print "Product diversity"
    keys = [
        ('user_id', 'item_id'),
        ('user_id', 'cat_id'),
        ('user_id', 'brand_id'),
        ('merchant_id', 'item_id'),
        ('merchant_id', 'cat_id'),
        ('merchant_id', 'brand_id'),
        ('user_id', 'merchant_id', 'item_id'),
        ('user_id', 'merchant_id', 'cat_id'),
        ('user_id', 'merchant_id', 'brand_id')
    ]
    _2p_sql = 'SELECT %s, %s, sum(action_0), sum(action_2), sum(action_3) FROM USERLOG GROUP BY %s, %s'
    _3p_sql = 'SELECT %s, %s, %s, sum(action_0), sum(action_2), sum(action_3) FROM USERLOG GROUP BY %s, %s, %s'
    _common_features = ['action_0', 'action_2', 'action_3']
    _dtype = {'action_0': np.int32, 'action_2': np.int32, 'action_3': np.int32}
    _2p_path = './inter/v3/count/product/%s_%s_product.csv'
    _3p_path = './inter/v3/count/product/%s_%s_%s_product.csv'
    params = [(
        key,
        _2p_sql % (key+key) if len(key) == 2 else _3p_sql % (key+key),
        _common_features,
        _dtype,
        _2p_path % key if len(key) == 2 else _3p_path % key,
        __insert_day_data
    ) for key in keys]
    conn = sqlite3.connect('./inter/features.db')

    for k, sql, fe, dt, p, handler in params:
        features = list(k)
        features.extend(fe)
        pd.DataFrame(columns=features).to_csv(p, index=False)
        __sql(conn, sql, p, CHUNKSIZE, handler, features, dt)
        print "Totally completed with %s" % str(k)

    conn.close()

# Penetration features
def penetration():
    """
    The number of users who have purchased an item, merchant, brand or category in a given time interval.
    Shows the customer base of each item, brand, category and merchant.
    """
    print "Penetration features"
    keys = [
        'item_id',
        'cat_id',
        'brand_id',
        'merchant_id'
    ]
    _sql = "SELECT %s, month, count(user_id), count(distinct(user_id)) FROM USERLOG WHERE action_type=2 GROUP BY %s, month"
    _common_features = ['month', 'user_num', 'distinct_user_num']
    _dtype = {'month': np.int32, 'user_num': np.int32, 'distinct_user_num': np.int32}
    _path = './inter/v3/count/penetration/%s_monthly_penetration.csv'
    conn = sqlite3.connect('./inter/features.db')

    for k, sql, fe, dt, p, handler in [(
                key,
                _sql % (key, key),
                _common_features,
                _dtype,
                _path % key,
                __insert_day_data
            ) for key in keys]:
        features = [k]
        features.extend(fe)
        pd.DataFrame(columns=features).to_csv(p, index=False)
        __sql(conn, sql, p, CHUNKSIZE, handler, features, dt)
        print "Totally completed with %s" % str(k)

    conn.close()

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
        df['month'] = df['time_stamp'].apply(lambda x: MONTH.index(x[:2]))
        df.apply(__monthly_record, axis=1, args=(df,))
        df = df.fillna(0)
        df['action_0'] += df['action_1']
        df.drop(['action_1'], axis=1)\
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
    # action_count(False)
    # map(day_count, [True, False])
    # pair_count()
    # product_diversity()
    penetration()

if __name__ == '__main__':
    main()

