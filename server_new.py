import socket
import argparse
import os
import shutil
import Settings
import pickle


def start_server():
    mapper, reducer, file, number = parse_command_line()
    print(f"mapper name: %s" % mapper)
    print(f"reducer name: %s" % reducer)
    print(f"file name: %s" % file)
    print(f"number of nodes: %s" % number)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # host = '192.168.1.68'  # 服务器的IP地址，0.0.0.0 表示接受来自任何网络接口的连接
    host = Settings.ServerIP()
    port = 12345  # 选择一个未被占用的端口

    server_socket.bind((host, port))
    server_socket.listen(int(number))

    print(f"Waiting for client to connect {host}:{port}...")

    # 建立与Clients的连接
    Clients = SetupClients(server_socket, number)

    buffer = []
    for i in range(len(Clients)):
        buffer.append([])

    # split the input file into different small part and send to clients
    os.makedirs('map_reduce_data')
    splitNumber = len(Clients)
    split_file_by_lines(file, splitNumber, 'map_reduce_data')

    # Sending data to reducers
    for i in range(splitNumber):
        send_file = f"map_reduce_data/reduce_part{i + 1}.txt"
        with open(send_file, 'rb') as file:
            data = file.read()
            Clients[i].client_socket.sendall(data)
    print("to_be_mapped successfully sent")

    data = None
    for i in range(len(Clients)):
        client_socket = Clients[i].client_socket
        received_data = client_socket.recv(1024)
        data = received_data
        path = "to_shuffle.txt"
        if not os.path.exists(path):
            with open(path, 'w') as file:
                file.writelines(data)
        else:
            # 如果文件存在，追加写入内容
            with open(path, 'a') as file:
                file.writelines(data)

    input("按下回车结束")
    shutil.rmtree('map_reduce_data')
    input("该关闭了")

    # 假装在shuffle
    # dict = {}
    # for iter in data:
    #     (key, value) = iter
    #     if key not in dict.keys():
    #         dict[key] = [1]
    #     else:
    #         dict[key].append(1)

    Shuffle("to_shuffle.txt", Clients, buffer)

    # # sending to reducers
    # for cli in Clients:
    #     file_to_be_sent = dict
    #     file_to_be_sent = pickle.dumps(file_to_be_sent)
    #     client_socket.sendall(file_to_be_sent)

    input("准备从reducer接收")
    sum = 0
    # receiving from reducers
    for i in range(len(Clients)):
        received_data = client_socket.recv(1024)
        data = pickle.loads(received_data)
        sum += data

    print(sum)
    input("按下回车结束")
    shutil.rmtree('map_reduce_data')

    ########################################################################################################################
    ########################################################################################################################

    # mapper, reducer, file, number = parse_command_line()
    # print(f"mapper name: %s" % mapper)
    # print(f"reducer name: %s" % reducer)
    # print(f"file name: %s" % file)
    # print(f"number of nodes: %s" % number)
    # server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # # host = '192.168.1.68'  # 服务器的IP地址，0.0.0.0 表示接受来自任何网络接口的连接
    # host = Settings.ServerIP()
    # port = 12345  # 选择一个未被占用的端口
    #
    # server_socket.bind((host, port))
    # server_socket.listen(int(number))
    #
    #
    # for i in range(0, int(number)):
    #     clientName = f"client_{i}"
    #     client_socket, client_address = server_socket.accept()
    #     currentClient = Client(clientName,client_socket, client_address)
    #     client.append(currentClient)
    #     print(f"Connected by {client_address}")
    #
    # # split the input file into different small part and send to clients
    # os.makedirs('map_reduce_data')
    # splitNumber = len(client)
    # split_file_by_lines(file, splitNumber, 'map_reduce_data')
    #
    #
    # print(f"Waiting for client to connect {host}:{port}...")
    # ShutdownFlag = False
    # Clients = []
    # while(True):    # 循环监听
    #
    #     # 关闭客户端连接
    #     if ShutdownFlag == True:
    #         client_socket.close()
    #
    #     # 等待连接
    #     client_socket, client_address = server_socket.accept()
    #     print('连接来自:', client_address)
    #     currentClient = Client(clientName, client_socket, client_address)
    #     client.append(currentClient)
    #     print(f"Connected by {client_address}")

def SetupClients(server_socket, num_clients):
    Clients = []
    for i in range(0, int(num_clients)):
        clientName = f"client_{i}"
        client_socket, client_address = server_socket.accept()
        currentClient = Client(clientName, client_socket, client_address)
        Clients.append(currentClient)
        print(f"Connected by {client_address}")
    return Clients



def Shuffle(to_be_shuffled, clients, buffer):
    input("Click to shuffle.")
    os.makedirs("shuffle")
    with open(to_be_shuffled, 'r') as file:
        # 使用 readlines() 方法将文件内容按行读入列表
        lines = [line.strip() for line in file.readlines()]
    data = []
    for line in lines:
        line = pickle.loads(line)
        data.append(line)
    dict = {}
    for (key, value) in data:
        if key not in dict.keys():
            dict[key] = []
            dict[key].append(value)
        else:
            dict[key].append(value)
    for key in dict.keys():
        hashed = (hash(key) % len(clients))
        buffer[hashed].append((key, dict[key]))
        if len(buffer[hashed]) > 5:
            path = "shuffle/to_shuffle_{}.txt".format(hashed)
            if not os.path.exists(path):
                with open(path, 'w') as file:
                    for i in range(5):
                        file.writelines(buffer[hashed][i])
            else:
                # 如果文件存在，追加写入内容
                with open(path, 'a') as file:
                    for i in range(5):
                        file.writelines(buffer[hashed][i])

    for i in range(len(buffer)):
        buffer_data = buffer[i]
        path = "shuffle/to_shuffle_{}.txt".format(i)
        if not os.path.exists(path):
            with open(path, 'w') as file:
                for iter in buffer[i]:
                    file.writelines(iter)
        else:
            # 如果文件存在，追加写入内容
            with open(path, 'a') as file:
                for iter in buffer[i]:
                    file.writelines(iter)
    for i in range(len(clients)):
        client_socket = clients[i].client_socket
        path = "shuffle/to_shuffle_{}.txt".format(i)
        with open(path, 'rb') as file:
            data = file.read()
            client_socket.sendall(data)
    shutil.rmtree('shuffle')
    shutil.rmtree('to_shuffle.txt')
    input("检查一下")
    shuffled = None
    return shuffled



def FinalProcess(data):
    input("Click to finish the final process")
    result = None
    return result


# 关闭某个client
def CloseClient(client, clients):
    for iter in clients:
        if iter.address == client.address:
            clients.remove(iter)
    return clients


class Client:
    def __init__(self, name, client_socket, client_address):
        self.name = name
        self.client_socket = client_socket
        self.client_address = client_address
        self.state = "idle"
        # states = ["idle", "mapper", "reducer"]

    def setState(self, state):
        self.state = state


def split_file_by_lines(input_file, number_of_split, output_dir):
    with open(input_file, 'r') as infile:
        all_lines = infile.readlines()
        total_lines = len(all_lines)
        lines_per_part = total_lines // number_of_split

        for part_num in range(number_of_split):
            start_index = part_num * lines_per_part
            end_index = (part_num + 1) * lines_per_part if part_num < lines_per_part - 1 else total_lines

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
    parser.add_argument('-n', '--number', help='Number of client nodes', required=True, default=1)

    # Parse the command-line arguments
    args = parser.parse_args()

    # Access the values using dot notation
    mapper = args.mapper
    reducer = args.reducer
    file = args.file
    number = args.number

    # Your program logic using input_file and output_file
    return mapper, reducer, file, number


if __name__ == "__main__":
    start_server()
