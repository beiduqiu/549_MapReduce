import string
import sys
import ast
import pandas as pd

def pd_2_tuples(file_address):
    df = pd.read_csv(file_address)
    tuples = [(row.Key, row.Value) for row in df.itertuples()]
    return tuples

def main():
    path = 'to_be_reduced.csv'
    pairs = pd_2_tuples(path)
    dict = {}
    for pair in pairs:
        (word, value) = pair
        if word not in dict.keys():
            dict[word] = 1
        else:
            dict[word] += 1
    for key in dict.keys():
        print((key, dict[key]))


if __name__ == '__main__':
    main()