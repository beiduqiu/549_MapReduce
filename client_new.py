import socket
import Settings
import pickle
import shutil
import os
from Utils import *

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server_host = '192.168.1.68'  # Server IP
    server_host = Settings.ServerIP()
    server_port = 12345            # Server Port

    client_socket.connect((server_host, server_port))

    input("Click to receive data from server.")

    file_received = client_socket.recv(1024)
    file_received = file_received.decode('utf-8')
    file_mapped = Map(file_received)

    input("Click to send mapped data to server.")
    client_socket.sendall(file_mapped)

    input("Click to start shuffling.")

    # file_to_send = "file_to_send.txt"
    # with open(file_to_send, 'rb') as file:
    #     for data in file:
    #         client_socket.sendall(data)

    # print(f"文件发送完成：{file_to_send}")

    file_received = client_socket.recv(1024)
    file_received = pickle.loads(file_received)
    data = Reduce(file_received)
    client_socket.sendall(data)
    input("reducer发送完毕")
    client_socket.close()

def Map(i, to_be_mapped):
    input("Click to finish mapping")
    filePath = "MappedData_{}.txt".format(i)
    mapped = getLog("mapper.py", filePath, to_be_mapped)
    result = None
    return result

def Reduce(i):
    input("Click to finish reducing")
    filePath = "ReducedData_{}.txt".format(i)
    reduced = getLog("reducer.py", filePath)
    result = None
    return result

def DeleteFile(path):
    shutil.rmtree(path)

def ReadFrom(path):
    with open(path, 'r') as file:
        # 使用 readlines() 方法将文件内容按行读入列表
        lines = [line.strip() for line in file.readlines()]
    data = lines
    return data

def WriteTo(path, data):
    # data should be a list
    if not os.path.exists(path):
        with open(path, 'w') as file:
            for i in range(len(data)):
                file.writelines(data[i])
    else:
        # 如果文件存在，追加写入内容
        with open(path, 'a') as file:
            for i in range(len(data)):
                file.writelines(data[i])

if __name__ == "__main__":
    start_client()
