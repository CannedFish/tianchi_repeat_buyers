# -*- coding: utf8 -*-

import pandas as pd
import numpy as np

CHUNKSIZE = 10 ** 6
MONTH = ['%02d' % x for x in xrange(5, 12)]
KEYs = ['user_id', 'item_id', 'cat_id', 'merchant_id', 'brand_id']

def monthly_count():
    """
    :param key: entity key
    """
    def __monthly_record(row, _df):
        _df.at[row.name, 'action_%d' % row['action_type']] = 1

    dfs = []
    batch = 0
    print "Reading & Reshaping data..."
    for df in pd.read_csv("./data/data_format1/user_log_format1.csv"
            , chunksize=CHUNKSIZE
            # , usecols=header
            , dtype={'user_id': str, 
                'item_id': str,
                'cat_id': str,
                'merchant_id': str,
                'brand_id': str,
                'time_stamp': str, 
                'action_type': np.int32}):
        df['time_stamp'] = df['time_stamp'].apply(lambda x: MONTH.index(x[:2]))
        df.apply(__monthly_record, axis=1, args=(df,))
        df['action_0'] += df['action_1']
        df.drop(['action_type', 'action_1'], axis=1)
        dfs.append(df.fillna(0))
        print "Batch %d completed" % batch
        batch += 1

    print "Aggragating data..."
    aggr_df = pd.concat(dfs)
    common_label = ['time_stamp', 'action_0', 'action_2', 'action_3']
    for key in KEYs:
        print "\nGenerate monthly count with %s" % key
        target_cols = [key]
        target_cols.extend(common_label)
        sub_df = aggr_df.select(lambda l: l in target_cols, axis=1)\
                .groupby([key, 'time_stamp'])\
                .sum()\
                .to_csv('./inter/v3/%s_monthly_action.csv' % key)
        # sub_df.reset_index(level=[key, 'time_stamp'], inplace=True)
        # sub_df.to_csv('./inter/v3/%s_monthly_action.csv' % key, index=False)
        print "Generate monthly count with %s completed." % key

    # huge_df = pd.concat(dfs)
    # keys = []
    # monthly = [[[] for __ in xrange(3)] for _ in xrange(5, 12)]
    # for g_idx, groupdf in huge_df.groupby([key]):
        # monthly_actions = [[0, 0, 0, 0] for _ in range(5, 12)]
        # for idx, row in groupdf.iterrows():
            # monthly_count['time_stamp']['action_type'] += 1

def main():
    monthly_count()

if __name__ == '__main__':
    main()

