from Utils import *
import string
import sys

def main(path):
    text = readFrom(path)
    text = text.replace('\n', ' ')
    text = ''.join(char for char in text if char not in string.punctuation)
    words = text.split(" ")
    for word in words:
        if word == "":
            continue
        print((word, 1))

if __name__ == '__main__':
    file_path = sys.argv[1]
    main(file_path)