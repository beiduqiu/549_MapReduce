import socket
import argparse
import os
import shutil
import threading
import time
import Settings
import pickle
import utils
import dask.bag as db

class MyServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.workers = []
        self.workers_states = []
        self.tasks = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lock = threading.Lock()
    
    def start_connection(self,num_nodes):
        #self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(num_nodes+10)
        num = 0
        while num < num_nodes:
            conn, addr = self.socket.accept()
            #conn.setblocking(False)
            print(f"Connect to worker {num}: {conn},{addr}")
            self.workers.append([conn,addr])
            self.workers_states.append("idle")
            #threading.Thread(target=self.handle_worker, args=(conn,)).start()
            num += 1
        #threading.Thread(target=self.monitor_workers()).start()

    def send_map_file(self,file_path,file_name,num_workers):
        with open(file_path, 'rb') as file:
            file_size = os.path.getsize(file_path)
            file_info = f"{file_name},{file_size}"
            for i in range(num_workers):
                self.workers[i][0].send(file_info.encode('utf8'))
            while True:
                data = file.read(1024)
                for i in range(num_workers):
                    self.workers[i][0].send(data)
                if not data:
                    break
        print("send map file to all workers")
        return 1
    
    def send_worker_list(self,num_workers):
        worker_addr = []
        for worker in self.workers:
            worker_addr.append(worker[1])
        print(worker_addr)
        serialized_data = pickle.dumps(worker_addr)
        work_addr_length = len(serialized_data)
        for i in range(num_workers):
            work_addr_length = f"{i},{work_addr_length}"
            self.workers[i][0].send(work_addr_length.encode('utf8'))
            self.workers[i][0].send(serialized_data)
        return 1

        
    def send_reduce_file(self,file_path,file_name,num_workers):
        with open(file_path, 'rb') as file:
            file_size = os.path.getsize(file_path)
            file_info = f"{file_name},{file_size}"
            for i in range(num_workers):
                self.workers[i][0].send(file_info.encode('utf8'))
            while True:
                data = file.read(1024)
                for i in range(num_workers):
                    self.workers[i][0].send(data)
                if not data:
                    break
        print("send reduce file to all workers")
        return 1
    
    def send_origin_data_to_worker(self, num_workers,input_file_name):
        for i in range(num_workers):
            send_file = f"splited_data/{i}_{input_file_name}"
            file_size = os.path.getsize(send_file)
            file_info = f"{input_file_name},{file_size}"
            self.workers[i][0].send(file_info.encode('utf8'))
            with open(send_file, 'rb') as file:
                while True:
                    data = file.read(1024)
                    self.workers[i][0].send(data)
                    if not data:
                        break
            self.workers_states[i] = "mapping"
        print("splited data successfully sent")

        return 1

    def handle_worker(self, conn):
        while True:
            data = conn.recv(1024)
            if not data:
                break
            message = data.decode('utf-8')
            worker_id, status = message.split(',')
            with self.lock:
                self.workers[worker_id] = status

    def assign_task(self, worker_id, task):
        with self.lock:
            self.tasks[worker_id] = task
        # 发送任务逻辑

    def monitor_workers(self):
        while True:
            time.sleep(5)
            with self.lock:
                for worker_id, status in list(self.workers.items()):
                    if status == 'idle':
                        pass

    def split_2_k(self, file_path, k):
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        chunk_size = len(lines) // k
        file_chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

        for i, chunk in enumerate(file_chunks):
            with open(f'File/file_part_{i}.txt', 'w', encoding='utf-8') as f:
                f.writelines(chunk)

    def check_client(self, worker_list):
        Flag = True
        for i in range(len(worker_list)):
            state = self.workers_states[i]
            if state != "idle":
                Flag = False
        return Flag

    def receive_status(self):
        print("asdfasdfasf")
        for i in range(len(self.workers)):
            print(i)
            client_socket = self.workers[i][0]
            #client_socket = self.socket
            print(-1)
            print(client_socket)
            data = client_socket.recv(1024)
            # data = 0
            # while True:
            #     print("666")
            #     if (data !=0):
            #         break
            #     try:
            #         data = client_socket.recv(1024)
            #     except BlockingIOError:
            #         # Handle non-blocking socket error
            #         pass
            print(0)
            status = data.decode('utf-8')
            print(status)
            self.workers_states[i] = status

    def send_signal(self, signal):
        for i in range(len(self.workers)):
            (ip, port) = self.workers[i]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, port))
                s.sendall(signal.encode('utf8'))


def split_file_by_lines(input_file_path,input_file_name, number_of_split,output_dir):
    with open(input_file_path, 'r') as infile:
        all_lines = infile.readlines()
        total_lines = len(all_lines)
        lines_per_part = total_lines // number_of_split
        
        for part_num in range(number_of_split):
            start_index = part_num * lines_per_part
            end_index = (part_num + 1) * lines_per_part if part_num < lines_per_part- 1 else total_lines

            # 构造输出文件名
            output_file = f"{output_dir}\{part_num}_{input_file_name}"

            # 将部分写入输出文件
            with open(output_file, 'w') as outfile:
                outfile.writelines(all_lines[start_index:end_index])
    return number_of_split
