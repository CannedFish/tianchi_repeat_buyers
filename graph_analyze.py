# -*- coding: utf8 -*-

# TODO: user features
# 1. traverse each repeat buyer's activity;
# 2. try to find thire common features(to all merchants or some dedicated);
# 3. build user model based on this features;
# 4. calculate the similarity between users;
def build_relationships():
    # TODO: connect repeat buyers to merchants
    # (:User)-[:Label {value: 0/1}]->(:Merchant)
    # NOTE: remove Labels first
    pass

def user_similarity():
    # TODO: measure how similar two users are
    pass

# TODO: merchant features
# 1. cluster users who took activities to each merchant;
def merchant_quality():
    # TODO: repeat_buyers / total_buyers
    pass

def probability():
    pass

def prepare_data():
    # TODO: split original training data to training data and validation data
    # and save in dedicated files: training_data.csv, validation_data.csv
    pass

def train(target):
    """
    :param target: float, [0, 1)
    """
    error = 1
    while error > target:
        prepare_data()
        build_relationships()
        # user_similarity() ?
        merchant_quality()

def main():
    pass

if __name__ == '__main__':
    main()

