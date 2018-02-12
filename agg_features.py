# -*- coding: utf8 -*-

import pandas as pd

from os import path

CHUNKSIZE = 10 ** 5

def __action_count():
    features = ['month', 'action_0', 'action_2', 'action_3', 'action_0_ratio', 'action_2_ratio', 'action_3_ratio']
    def __walk_fn(arg, dirname, filenames):
        # TODO: 
        # 1. calculate mean, max, median;
        # 2. calculate standard deviation by myself:
            # 2.1 calc sum((value - mean) ^ 2)
            # 2.2 calc sqrt(sum(each_batch_out) / (total_num - 1))
        tar_files = filter(lambda n: n.find('monthly') >= 0, filenames)
        for filename in tar_files:
            df_mean = pd.DataFrame()
            df_max = pd.DataFrame()
            df_median = pd.DataFrame()
            for df in pd.read_csv(path.join(dirname, filename), chunksize=CHUNKSIZE, usecols=arg):
                group = df.groupby(['month'])
                df_mean = df_mean.append(group.mean())
                df_max = df_max.append(group.max())
                df_median = df_median.append(group.median())

def agg_monthly():
    __action_count()

def main():
    pass

if __name__ == '__main__':
    main()

