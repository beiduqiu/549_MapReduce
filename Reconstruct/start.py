from server import *
from client import *
from Settings import *
import multiprocessing
import subprocess
import os
import shutil

def run_script(script_name):
    subprocess.run(['python', script_name])

def main():
    host = ServerIP()
    port = 12345
    print(host)
    my_server = MyServer(host, port)
    num_workers = 1
    #worker1 = Worker(host, port, 'worker1')
    #worker2 = Worker(host, port, 'worker2')
    my_server.start_connection(num_workers)
    os.makedirs('splited_data')
    split_file_by_lines("User\\data.txt","data.txt",len(my_server.workers),"splited_data")
    my_server.send_origin_data_to_worker(num_workers,"data.txt")
    shutil.rmtree('splited_data')
    # 启动更多 workers ...
'''
    # Workers 开始发送状态和接收任务
    worker1.send_status()
    worker1.receive_task()
    worker2.send_status()
    worker2.receive_task()'''

if __name__ == '__main__':
    main()