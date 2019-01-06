import sys

def read_file():
    f = open(sys.argv[1], 'r')
    s = f.read()
    f.close()

    return s
    
if __name__ == '__main__':
    jsonBody = read_file()
    print(jsonBody)

    
