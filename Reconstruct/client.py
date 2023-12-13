import socket
import time
import utils
import pandas as pd
import Settings

class Worker:
    def __init__(self, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port
        self.worker_id = -1
        self.status = "idle"
        ## ['idle', 'mapping', 'reducing']
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_status(self):
        while True:
            time.sleep(10)
        message = f"{self.worker_id}, {self.status}"
        self.connect.sendall(message.encode('utf8'))

    def receive_task(self):
        while True:
            data = self.conn.recv(1024)
            if not data:
                break
            task = data.decode('utf-8')
            # 处理任务逻辑
    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_host = '192.168.1.68'  # Server IP
        server_host = Settings.ServerIP()
        server_port = 12345            # Server Port

        client_socket.connect((self.server_host, self.server_port))

        '''input("Click to receive data from server.")

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
        input("reducer发送完毕")'''
        client_socket.close()

    def run(self):
        # 运行环境下用户代码
        pass

    def map(self, file_path):
        map_log = utils.getLog("mapper.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        df.sort_values(by='Key')
        df.to_csv(file_path)
        utils.deleteFile('tuples.txt')

    def reduce(self, addr_list, key_list):
        utils.combine(addr_list, key_list)
        reduce_log = utils.getLog("reducer.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        df.sort_values(by='Key')
        df.to_csv("reduced.csv")
        utils.deleteFile('tuples.txt')
        utils.deleteFile('to_be_reduced.csv')

if __name__ == "__main__":
    
    server_host = Settings.ServerIP()
    server_port = 12345
    worker = Worker(server_host,server_port)
    worker.start_client()
