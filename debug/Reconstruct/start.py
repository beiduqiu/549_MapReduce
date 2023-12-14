from server import *
from client import *
from Settings import *
import multiprocessing
import subprocess
import os
import time
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
    time.sleep(0.1)
    my_server.send_worker_list(num_workers)
    time.sleep(0.1)
    my_server.send_reduce_file("User\\reducer.py","reducer.py",num_workers)
    time.sleep(0.1)
    my_server.send_map_file("User\\mapper.py","mapper.py",num_workers)
    os.makedirs('splited_data')
    split_file_by_lines("User\\data.txt","data.txt",len(my_server.workers),"splited_data")
    my_server.send_origin_data_to_worker(num_workers,"data.txt")
    shutil.rmtree('splited_data')
    time.sleep(1)
    my_server.workers[0][0].shutdown(socket.SHUT_WR)
    my_server.receive_status()
    print(my_server.workers_states)

if __name__ == '__main__':
    main()