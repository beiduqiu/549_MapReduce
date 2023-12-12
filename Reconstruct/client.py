import socket
import time

class Worker:
    def __init__(self, server_host, server_port, worker_id):
        self.server_host = server_host
        self.server_port = server_port
        self.worker_id = worker_id
        self.status = "IDLE"
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
