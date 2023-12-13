import socket
import time
import utils
import pandas as pd

class Worker:
    def __init__(self, server_host, server_port, worker_id):
        self.server_host = server_host
        self.server_port = server_port
        self.worker_id = worker_id
        self.status = "idle"
        ## ['idle', 'mapping', 'reducing']
        self.connect = socket.create_connection((self.server_host, self.server_port))

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