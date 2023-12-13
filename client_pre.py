import socket
import Settings
import pickle
import shutil

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server_host = '192.168.1.68'  # 服务器的IP地址或主机名
    server_host = Settings.ServerIP()
    server_port = 12345            # 服务器的端口号

    client_socket.connect((server_host, server_port))

    input("Click to receive data from server.")

    file_received = client_socket.recv(1024)
    file_received = file_received.decode('utf-8')
    file_mapped = Map(file_received)

    input("Click to send mapped data to server.")
    client_socket.sendall(file_mapped)

    input("等待shuffle")

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

def Map(to_be_mapped):
    input("Click to finish mapping")
    result = []
    for char in to_be_mapped:
        result.append((char, 1))
    to_send = pickle.dumps(result)
    mapped = to_send
    return mapped

def Reduce(to_be_reduced):
    input("Click to finish reducing")
    data = to_be_reduced
    sum = 0
    for keys in data.keys():
        value = data[keys]
        for iter in value:
            iter = int(iter)
            sum += iter
    reduced = sum
    reduced = pickle.dumps(reduced)
    return reduced

def DeleteFile(path):
    shutil.rmtree(path)

if __name__ == "__main__":
    start_client()
