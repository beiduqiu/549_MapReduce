import socket
import argparse
import os
import shutil
import threading
import time
import Settings
import pickle

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.workers = {}
        self.tasks = {}
        self.lock = threading.Lock()

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        # self.socket.listen(5)
        threading.Thread(target=self.monitor_workers()).start()

        while True:
            conn, addr = self.socket.accept()
            threading.Thread(target=self.handle_worker, args=(conn,)).start()


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

