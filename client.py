import socket

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_host = '192.168.1.68'  # 服务器的IP地址或主机名
    server_port = 12345            # 服务器的端口号

    client_socket.connect((server_host, server_port))

    file_to_send = "file_to_send.txt"
    with open(file_to_send, 'rb') as file:
        for data in file:
            client_socket.sendall(data)

    print(f"文件发送完成：{file_to_send}")
    client_socket.close()

if __name__ == "__main__":
    start_client()
