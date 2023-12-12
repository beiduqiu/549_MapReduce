from server import *
from client import *
from Settings import *
import multiprocessing
import subprocess

def run_script(script_name):
    subprocess.run(['python', script_name])

def main():
    host = Settings.ServerIP()
    port = 12345
    server = Server(host, port)
    server.start()

    ## Setup clients
    worker1 = Worker(host, port, 'worker1')
    worker2 = Worker(host, port, 'worker2')
    # 启动更多 workers ...

    # Workers 开始发送状态和接收任务
    worker1.send_status()
    worker1.receive_task()
    worker2.send_status()
    worker2.receive_task()

if __name__ == '__main__':
    main()