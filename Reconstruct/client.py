import socket
import time
import utils
import pandas as pd
import Settings
import hashlib

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

    '''def receive_task(self):
        f = open("Client\\data.txt", "wb")
        while True:
            data = self.socket.recv(1024)
            f.write(data)
            if not data:
                break
    def receive_map_file(self):
        f = open("mapper.py", "wb")
        while True:
            data = self.socket.recv(1024)
            f.write(data)
            if not data:
                break
    def receive_reduce_file(self):
        f = open("reducer.py", "wb")
        while True:
            data = self.socket.recv(1024)
            f.write(data)
            if not data:
                break'''
    def receive_file(self,client_socket, file_name, file_size):
        file_path = f"Client\\{file_name}"
        with open(file_path, 'wb') as file:
            remaining_size = file_size
            while remaining_size > 0:
                data = client_socket.recv(min(1024, remaining_size))
                if not data:
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
       

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_host = '192.168.1.68'  # Server IP
        server_host = Settings.ServerIP()
        server_port = 12345            # Server Port

        self.socket.connect((self.server_host, self.server_port))
        self.receive_files(self.socket)
        self.receive_files(self.socket)
        self.receive_files(self.socket)
        self.map(1)

    def run(self):
        # 运行环境下用户代码
        pass

    def map(self, k):
        map_log = utils.getLog("mapper.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        sorted_df = df.sort_values(by='Key')
        file_path = "mapped-{}.csv".format(self.worker_id)
        sorted_df.to_csv(file_path, index=False)
        utils.deleteFile('tuples.txt')
        self.shuffle(sorted_df, k)



    def shuffle(self, df, k):
        keys = df['Key'].unique().tolist()
        buckets = {i: [] for i in range(k)}
        for key in keys:
            bucket_num = hash(key) % k
            buckets[bucket_num].append(key)

        for key in buckets.keys():
            keys = buckets[key]
            filtered_df = df[df['Key'].isin(keys)]
            filtered_df.to_csv('mapped-{}-part-{}.csv'.format(self.worker_id, key), index=False)

    def distribute(self, addr_dict, k):
        i = self.worker_id
        for j in range(k):
            file = "mapped-i-part-{}.csv".format(j)
            client = addr_dict[j]
            utils.sendFile(client, file)



    def reduce(self, addr_list):
        utils.combine(addr_list)
        reduce_log = utils.getLog("reducer.py", "tuples.txt")
        tuples = utils.readTuple("tuples.txt")
        df = utils.tuples_2_pd(tuples)
        df.sort_values(by='Key')
        df.to_csv("reduced-{}.csv".format(self.worker_id), index=False)
        utils.deleteFile('tuples.txt')
        utils.deleteFile('to_be_reduced.csv')



if __name__ == "__main__":
    
    server_host = Settings.ServerIP()
    server_port = 12345
    worker = Worker(server_host,server_port)
    worker.start_client()
    print("hhh")
