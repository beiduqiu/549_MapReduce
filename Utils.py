import subprocess
import shutil
import ast


def readFrom(path):
    with open(path, 'r') as file:
        data = file.read()
    result = data
    return result


def readLine(path):
    data = []
    with open(path, 'r') as file:
        for line in file:
            data.append(line.strip())
    result = data
    return result


def getLog(program, path, data):
    # save the log of program to path
    result = subprocess.run(['python', program], capture_output=True, text=True)
    with open(path, 'w') as file:
        file.write(result.stdout)
    return result.stdout


def deleteFile(path):
    shutil.rmtree(path)

def readTuple(path):
    pairs = readLine(path)
    tuples = []
    for pair in pairs:
        tuple = ast.literal_eval(pair)
        tuples.append(tuple)
    result = tuples
    return result
