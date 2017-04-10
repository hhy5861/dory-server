import dory
import sys
import socket

mc = dory.DoryMsgCreator()


class SendToDory(object):

    def send(self, message, socketPath, topic, msgKey=''):
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

        dory_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            dory_sock.sendto(any_partition_msg, socketPath)
        except socket.error as x:
            sys.stderr.write('Error sending UNIX datagram to Dory: ' + x.strerror + \
                             '\n')
            sys.exit(1)
        finally:
            dory_sock.close()
