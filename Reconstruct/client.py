import socket
import time
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
        f = open("Client\\data.txt", "wb")
        while True:
            data = self.socket.recv(1024)
            f.write(data)
            if not data:
                break
        
       

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_host = '192.168.1.68'  # Server IP
        server_host = Settings.ServerIP()
        server_port = 12345            # Server Port

        self.socket.connect((self.server_host, self.server_port))


    def run(self):
        # 运行环境下用户代码
        pass

if __name__ == "__main__":
    
    server_host = Settings.ServerIP()
    server_port = 12345
    worker = Worker(server_host,server_port)
    worker.start_client()
    print("hhh")
    worker.receive_task()