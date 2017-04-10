# coding=utf-8
import socket
from services import send
import threading, getopt, sys, string

opts, args = getopt.getopt(sys.argv[1:], "hp:l:s:t:a", ["help", "port=", "list=", "socket-path=", "topic=", "address="])
list = 0
port = 9010
host = '0.0.0.0'
socketPath = '/var/run/dory/dory.socket'
topic = 'test'
msgKey = ''


def usage():
    print """
    -h --help             print the help
    -l --list             Maximum number of connections
    -p --port             To monitor the port number  
    -s --socket-path      dory run socket path
    -t --topic            kafka create broker name
    -a --address          host connetion address
    """


for op, value in opts:
    if op in ("-l", "--list"):
        list = string.atol(value)
    elif op in ("-p", "--port"):
        port = string.atol(value)
    elif op in ("-a", "--address"):
        host = string.atol(value)
    elif op in ("-s", "--socket-path"):
        socketPath = string.atol(value)
    elif op in ("-t", "--topic"):
        topic = string.atol(value)
    elif op in ("-h", "-help"):
        usage()
        sys.exit()

dory = send.SendToDory()


def jonnyS(client, address):
    try:
        # client.settimeout(500)
        messages = client.recv(2048)
        dory.send(messages, socketPath, topic, msgKey)
    except socket.timeout:
        print 'time out'
        client.close()


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(list)
    while True:
        client, address = sock.accept()
        thread = threading.Thread(target=jonnyS, args=(client, address))
        thread.start()


if __name__ == '__main__':
    main()
