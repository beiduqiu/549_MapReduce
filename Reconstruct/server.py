import socket
import argparse
import os
import shutil
import threading
import time
import Settings
import pickle
import utils

class MyServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.workers = []
        self.tasks = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lock = threading.Lock()
    
    def start(self):
        #self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(2)
        num = 0
        while num < 2:
            conn, addr = self.socket.accept()
            print(f"Connect to worker {num}: {conn},{addr}")
            self.workers.append([conn,addr])
            #threading.Thread(target=self.handle_worker, args=(conn,)).start()
            num += 1
        #threading.Thread(target=self.monitor_workers()).start()
    def send_origin_data_to_worker():
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

    def shuffle(self, addr_list):
        sorted = utils.sort(addr_list)
        keys = sorted['Key'].unique().tolist()

        idle_workers = []
        for worker in self.workers.keys():
            if self.workers[worker] == 'idle':
                idle_workers.append(worker)

        num = len(idle_workers)
        buckets = {i: [] for i in range(num)}
        for key in keys:
            bucket_num = hash(key) % num
            buckets[num].append(key)

def split_file_by_lines(input_file, number_of_split,output_dir):
    with open(input_file, 'r') as infile:
        all_lines = infile.readlines()
        total_lines = len(all_lines)
        lines_per_part = total_lines // number_of_split
        
        for part_num in range(number_of_split):
            start_index = part_num * lines_per_part
            end_index = (part_num + 1) * lines_per_part if part_num < lines_per_part- 1 else total_lines

            # 构造输出文件名
            output_file = f"{output_dir}/reduce_part{part_num}.txt"

            # 将部分写入输出文件
            with open(output_file, 'w') as outfile:
                outfile.writelines(all_lines[start_index:end_index])
    return number_of_split
