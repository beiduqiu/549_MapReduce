from Utils import *
import string
import sys

def main(path):
    pairs = readTuple(path)
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
    file_path = sys.argv[1]
    main(file_path)