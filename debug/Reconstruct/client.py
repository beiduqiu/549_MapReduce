import socket
import time
import utils
import pandas as pd
import Settings
import pickle
import hashlib

class Worker:
    def __init__(self, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port
        self.worker_id = -1
        self.workers = []
        self.status = "idle"
        ## ['idle', 'mapping', 'reducing']
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def receive_file(self,client_socket, file_name, file_size):
        file_path = f"Client\\{file_name}"
        with open(file_path, 'wb') as file:
            remaining_size = file_size
            while remaining_size > 0:
                print("check point1")
                data = client_socket.recv(min(1024, remaining_size))
                print(remaining_size, data)
                if not data:
                    print("ddee")
                    break
                file.write(data)
                remaining_size -= len(data)

    def receive_files(self,client_socket):
        while True:
            # Receive file information (header)
            file_info = client_socket.recv(1024).decode('utf-8')
            if not file_info:
                break
            print(file_info)
            file_name, file_size = file_info.split(',')
            file_size = int(file_size)

            print(f"Receiving file: {file_name} ({file_size} bytes)")
            
            # Receive the file content
            self.receive_file(client_socket, file_name, file_size)
       
    def receive_work_list(self,client_socket):
            file_info = client_socket.recv(1024).decode('utf-8')
            print(file_info)
            worker_id,work_list_length = file_info.split(',')
            self.worker_id = worker_id
            print(f"Receiving work list: worker {self.worker_id}  ({work_list_length} bytes)")
            serialized_data = client_socket.recv(4096)
            data_list = pickle.loads(serialized_data)
            self.workers = data_list[:]
            print(self.workers)

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_host = '192.168.1.68'  # Server IP
        server_host = Settings.ServerIP()
        server_port = 12345            # Server Port

        self.socket.connect((self.server_host, self.server_port))
        self.receive_work_list(self.socket)
        self.receive_files(self.socket)
        self.receive_files(self.socket)
        self.receive_files(self.socket)
        print("gg")
        self.map()

    def run(self):
        # 运行环境下用户代码
        pass

    def map(self):
        self.status = "mapping"
        print(self.status)
        map_log = utils.getLog("mapper.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        sorted_df = df.sort_values(by='Key')
        file_path = "mapped-{}.csv".format(self.worker_id)
        sorted_df.to_csv(file_path, index=False)
        utils.deleteFile('tuples.txt')
        self.shuffle(sorted_df)
        self.status = "idle"
        self.send_status(self.status)



    def shuffle(self, df):
        k = len(self.workers)
        keys = df['Key'].unique().tolist()
        buckets = {i: [] for i in range(k)}
        for key in keys:
            bucket_num = hash(key) % k
            buckets[bucket_num].append(key)

        for key in buckets.keys():
            keys = buckets[key]
            filtered_df = df[df['Key'].isin(keys)]
            filtered_df.to_csv('mapped-{}-part-{}.csv'.format(self.worker_id, key), index=False)


    def reduce(self, addr_list):
        utils.combine(addr_list)
        reduce_log = utils.getLog("reducer.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        df.sort_values(by='Key')
        df.to_csv("reduced-{}.csv".format(self.worker_id), index=False)
        utils.deleteFile('tuples.txt')
        utils.deleteFile('to_be_reduced.csv')

    def send_status(self, status):
        host = self.server_host
        port = self.server_port
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     s.connect((host, port))
        #     s.sendall(status.encode('utf8'))
        message = f"{self.worker_id}, {self.status}"
        print("131231231")
        print(self.socket)
        x = self.socket.sendall(status.encode('utf8'))
        print(x)

    def receive_signal(self, server):
        (host, port) = server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))

            # 接收来自服务器的信号
            data = s.recv(1024)
            signal = data.decode('utf8')
            return signal

    def send_files(self):
        for i in range(len(self.workers)):
            if i == self.worker_id:
                continue
            client = self.workers[i]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(client)
                file = "mapped-{}-part-{}.csv".format(self.id, i)



if __name__ == "__main__":
    
    server_host = Settings.ServerIP()
    server_port = 12345
    worker = Worker(server_host,server_port)
    worker.start_client()
    print("hhh")
