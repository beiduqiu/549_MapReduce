import dask.dataframe as dd
import pandas as pd

def sort(address_list):
    # address_list contains the addresses of all the intermediate <key, value> pairs from mappers
    df = dd.read_csv(address_list)
    sorted_df = df.sort_values('Key')
    sorted_df.to_csv("asdasd.csv")


def tuples_2_pd(tuple_list, file_address):
    df = pd.DataFrame(tuple_list, columns=['Key', 'Value'])
    return pd

def pd_2_tuples(file_address):
    df = pd.read_csv(file_address)
    tuples = [tuple(x) for x in df.to_numpy()]
    return tuples
