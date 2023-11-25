import socket
import os
import shutil

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_host = '192.168.1.68'  # 服务器的IP地址或主机名
    server_port = 12345            # 服务器的端口号
    client_socket.connect((server_host, server_port))
    os.mkdir('client_map_reduce_data')
    receive_file(client_socket,'client_map_reduce_data')


    shutil.rmtree('client_map_reduce_data')
def receive_file(client_socket, save_path):
    save_path = f"client_map_reduce_data/reduce_data.txt"
    with open(save_path, 'wb') as file:
        data = client_socket.recv(1024)
        while data:
            file.write(data)
            data = client_socket.recv(1024)
def mapper():
    todo()
def reducer():
    todo()

if __name__ == "__main__":
    start_client()
