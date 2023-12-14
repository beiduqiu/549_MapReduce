import dask.dataframe as dd
import pandas as pd
import subprocess
import ast
import shutil
import os

def sort(address_list):
    # address_list contains the addresses of all the intermediate <key, value> pairs from mappers
    df = dd.read_csv(address_list)
    sorted_df = df.sort_values('Key')
    return sorted_df


def tuples_2_pd(tuple_list):
    df = pd.DataFrame(tuple_list, columns=['Key', 'Value'])
    return df

def pd_2_tuples(file_address):
    df = pd.read_csv(file_address)
    tuples = [(row.Key, row.Value) for row in df.itertuples()]
    return tuples

def getLog(program, path):
    # save the log of program to path
    result = subprocess.run(['python', program], capture_output=True, text=True)
    with open(path, 'w') as file:
        file.write(result.stdout)
    return result.stdout

def readTuple(path):
    pairs = readLine(path)
    tuples = []
    for pair in pairs:
        tuple = ast.literal_eval(pair)
        tuples.append(tuple)
    result = tuples
    return result

def readLine(path):
    data = []
    with open(path, 'r') as file:
        for line in file:
            data.append(line.strip())
    result = data
    return result

def deleteFile(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def combine(addr_list):
    combined_data = pd.DataFrame()
    for addr in addr_list:
        df = pd.read_csv(addr)

        combined_data = pd.concat([combined_data, df])
    sorted_df = combined_data.sort_values(by='Key')
    sorted_df.to_csv('to_be_reduced.csv', index=False)

def sendFile(client, file_path):
    return 1