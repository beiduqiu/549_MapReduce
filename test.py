# import pickle
# import os
# import json
#
# def ReadFrom(path):
#     with open(path, 'r') as file:
#         # 使用 readlines() 方法将文件内容按行读入列表
#         lines = [line.strip() for line in file.readlines()]
#     data = lines
#     return data
#
# # path = "command.txt"
# # print(ReadFrom(path))
#
# def LoadFrom(path):
#     with open(path, 'r') as file:
#         # 使用 readlines() 方法将文件内容按行读入列表
#         data = json.load(file)
#     return data
#
# path_new = "write_test.txt"
# data = [1, 2, 3]
# def WriteTo(path, data):
#     # data should be a list of pickled data
#     if not os.path.exists(path):
#         with open(path, 'w', encoding='utf-8') as file:
#             json.dump(data, file, indent=2)
#     else:
#         # 如果文件存在，追加写入内容
#         with open(path, 'a', encoding='utf-8') as file:
#             json.dump(data, file, indent=2)
# WriteTo(path_new, data)
# print(LoadFrom(path_new))
#


import subprocess

# 运行程序A并捕获输出
result = subprocess.run(['python', 'mapper.py', "data.txt"], capture_output=True, text=True)

# 将输出保存到文件
with open('mapper.txt', 'w') as f:
    f.write(result.stdout)

# 运行程序A并捕获输出
result = subprocess.run(['python', 'reducer.py', "mapper.txt"], capture_output=True, text=True)

# 将输出保存到文件
with open('reducer.txt', 'w') as f:
    f.write(result.stdout)

# 打印程序A的输出
print(result.stdout)