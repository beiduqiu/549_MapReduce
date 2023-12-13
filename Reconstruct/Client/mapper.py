import string
import sys

def readFrom(path):
    with open(path, 'r') as file:
        data = file.read()
    result = data
    return result

def main():
    text = readFrom('data.txt')
    text = text.replace('\n', ' ')
    text = ''.join(char for char in text if char not in string.punctuation)
    words = text.split(" ")
    for word in words:
        if word == "":
            continue
        print((word, 1))

if __name__ == '__main__':
    main()