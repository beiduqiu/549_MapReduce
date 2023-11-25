import socket
import argparse
import os
import shutil

def start_server():
    mapper, reducer,file,number = parse_command_line()
    print(f"mapper name: %s" % mapper)
    print(f"reducer name: %s" % reducer)
    print(f"file name: %s" % file)
    print(f"number of nodes: %s" % number)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = '192.168.1.68'  # 服务器的IP地址，0.0.0.0 表示接受来自任何网络接口的连接
    port = 12345       # 选择一个未被占用的端口

    server_socket.bind((host, port))
    server_socket.listen(int(number))  

    print(f"Waiting for client to connect {host}:{port}...")
    client = []
    #connect to different clients
    for i in range(0, int(number)):
        clientName = f"client_{i}"
        client_socket, client_address = server_socket.accept()
        currentClient = Client(clientName,client_socket, client_address)
        client.append(currentClient)
        print(f"Connected by {client_address}")
    #split the input file into different small part and send to clients
    os.makedirs('map_reduce_data')
    splitNumber = len(client)
    split_file_by_lines(file,splitNumber,'map_reduce_data')

    for i in range(splitNumber):
        send_file = f"map_reduce_data/reduce_part{i+1}.txt"
        with open(send_file,'rb') as file:
            data = file.read()
            print(data)
            client[i].client_socket.sendall(data)
    shutil.rmtree('map_reduce_data')


class Client:
    def __init__(self,name,client_socket,client_address):
        self.name = name
        self.client_socket = client_socket
        self.client_address = client_address

def split_file_by_lines(input_file, number_of_split,output_dir):
    with open(input_file, 'r') as infile:
        all_lines = infile.readlines()
        total_lines = len(all_lines)
        lines_per_part = total_lines // number_of_split
        
        for part_num in range(number_of_split):
            start_index = part_num * lines_per_part
            end_index = (part_num + 1) * lines_per_part if part_num < lines_per_part- 1 else total_lines

            # 构造输出文件名
            output_file = f"{output_dir}/reduce_part{part_num + 1}.txt"

            # 将部分写入输出文件
            with open(output_file, 'w') as outfile:
                outfile.writelines(all_lines[start_index:end_index])
    return number_of_split

def parse_command_line():
    parser = argparse.ArgumentParser(description='Description of your program.')
    # Add command-line arguments
    parser.add_argument('-m', '--mapper', help='Path to the mapper file ', required=True)
    parser.add_argument('-r', '--reducer', help='Path to the reducer file', required=True)
    parser.add_argument('-f', '--file', help='Path to the input file', required=True)
    parser.add_argument('-n', '--number', help='Number of client nodes', required=True,default=1)

    # Parse the command-line arguments
    args = parser.parse_args()

    # Access the values using dot notation
    mapper = args.mapper
    reducer = args.reducer
    file = args.file
    number = args.number

    # Your program logic using input_file and output_file
    return mapper, reducer,file,number
if __name__ == "__main__":
    start_server()
