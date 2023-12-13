import utils
import dask.bag as db



def map(file_path):
    map_log = utils.getLog("mapper.py", "tuples.txt")
    tuples = utils.readTuple("tuples.txt")
    df = utils.tuples_2_pd(tuples)
    df.sort_values(by='Key')
    df.to_csv(file_path)

# 示例用法
map('mapped.csv')
print(utils.pd_2_tuples('mapped.csv'))