# coding=utf-8
import socket
from services import dory
import threading, getopt, sys, string

opts, args = getopt.getopt(sys.argv[1:], "hp:l:s:t:a", ["help", "port=", "list=", "socket-path=", "topic=", "address="])
list = 50
port = 9010
host = '0.0.0.0'
socketPath = '/root/dory.socket'
topic = 'test'
msgKey = ''

mc = dory.DoryMsgCreator()
dory_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)


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


def jonnyS(client, address):
    try:
        client.settimeout(500)
        buf = client.recv(2048)
        print 'conn to address: {}'.format(address)
        print 'get message: {}'.format(buf)
        client.send(buf)
        # sendToDory(buf, socketPath, topic)
    except socket.timeout:
        print 'time out'
    client.close()


def sendToDory(message, socketPath, topic, msgKey=''):
    try:
        any_partition_msg = mc.create_any_partition_msg(topic,
                                                        mc.GetEpochMilliseconds(),
                                                        bytes(msgKey),
                                                        bytes(message)
                                                        )
    except dory.DoryTopicTooLarge as x:
        sys.stderr.write(str(x) + '\n')
        sys.exit(1)
    except dory.DoryMsgTooLarge as x:
        sys.stderr.write(str(x) + '\n')
        sys.exit(1)

    try:
        dory_sock.sendto(any_partition_msg, socketPath)
    except socket.error as x:
        sys.stderr.write('Error sending UNIX datagram to Dory: ' + x.strerror + \
                         '\n')
        sys.exit(1)
    finally:
        dory_sock.close()


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
