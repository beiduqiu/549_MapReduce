from utils import *
import subprocess
import utils

# tuple = [(1, 2), (2, 3), ('a', 'b')]
# # tuples_2_pd(tuple, 'Client/Client_2/to_be_shuffled.csv')
# add = ['Client/Client_1/to_be_shuffled.csv', 'Client/Client_2/to_be_shuffled.csv']
# sorted = sort(add)


def map(file_path):
    map_log = utils.getLog("mapper.py", "tuples.txt")
    tuples = utils.readTuple("tuples.txt")
    df = utils.tuples_2_pd(tuples)
    sorted = df.sort_values(by='Key')
    sorted.to_csv(file_path)
    utils.deleteFile('tuples.txt')

def reduce(addr_list, key_list, file_path):
    utils.combine(addr_list, key_list)
    reduce_log = utils.getLog("reducer.py", "tuples.txt")
    tuples = utils.readTuple("tuples.txt")
    df = utils.tuples_2_pd(tuples)
    df.sort_values(by='Key')
    df.to_csv(file_path)
    utils.deleteFile('tuples.txt')

map('mapped.csv')
addr = ['mapped.csv']
key_list = ['As', 'Enchantia']
reduce(addr, key_list, 'reduced.csv')
