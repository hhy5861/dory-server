#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Copyright 2014 if(we)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----------------------------------------------------------------------------
#
# This is an example Python script that sends messages to Dory.

import io
import struct
import time


class DoryTopicTooLarge(Exception):
    'exception thrown when Kafka topic is too large'

    def __init__(self):
        Exception.__init__(self, 'Kafka topic is too large')


class DoryMsgTooLarge(Exception):
    'exception thrown when message would exceed max possible size'

    def __init__(self):
        Exception.__init__(self, 'Cannot create a message that large')


class DoryMsgCreator(object):
    '''Class for creating datagrams to be sent to Dory.'''

    MSG_SIZE_FIELD_SIZE = 4
    API_KEY_FIELD_SIZE = 2
    API_VERSION_FIELD_SIZE = 2
    FLAGS_FIELD_SIZE = 2
    PARTITION_KEY_FIELD_SIZE = 4
    TOPIC_SIZE_FIELD_SIZE = 2
    TIMESTAMP_FIELD_SIZE = 8
    KEY_SIZE_FIELD_SIZE = 4
    VALUE_SIZE_FIELD_SIZE = 4

    ANY_PARTITION_FIXED_BYTES = MSG_SIZE_FIELD_SIZE + API_KEY_FIELD_SIZE + \
                                API_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE + \
                                TOPIC_SIZE_FIELD_SIZE + TIMESTAMP_FIELD_SIZE + \
                                KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE

    PARTITION_KEY_FIXED_BYTES = ANY_PARTITION_FIXED_BYTES + \
                                PARTITION_KEY_FIELD_SIZE

    ANY_PARTITION_API_KEY = 256
    ANY_PARTITION_API_VERSION = 0

    PARTITION_KEY_API_KEY = 257
    PARTITION_KEY_API_VERSION = 0

    # This is the maximum topic size allowed by Kafka.
    @staticmethod
    def getMaxTopicSize():
        return (2 ** 15) - 1

    # This is an extremely loose upper bound, based on the maximum value that
    # can be stored in a 32-bit signed integer field.  The actual maximum is
    # much smaller.  If we are sending to Dory by UNIX domain datagram socket,
    # we are limited by the maximum UNIX domain datagram size supported by the
    # operating system, which has been observed to be 212959 bytes on a CentOS
    # 7 x86_64 system.  If we are sending to Dory by UNIX domain stream socket
    # or local TCP, there is a configurable maximum imposed by the Kafka
    # brokers.  See the message.max.bytes setting in the Kafka broker
    # configuration.
    @staticmethod
    def getMaxMsgSize():
        return (2 ** 31) - 1

    def __init__(self):
        pass

    def create_any_partition_msg(self, topic, timestamp, key_bytes,
                                 value_bytes):
        topic_bytes = bytes(topic)

        if len(topic_bytes) > DoryMsgCreator.getMaxTopicSize():
            raise DoryTopicTooLarge()

        msg_size = DoryMsgCreator.ANY_PARTITION_FIXED_BYTES + \
                   len(topic_bytes) + len(key_bytes) + len(value_bytes)

        if msg_size > DoryMsgCreator.getMaxMsgSize():
            raise DoryMsgTooLarge()

        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhh', msg_size,
                              DoryMsgCreator.ANY_PARTITION_API_KEY,
                              DoryMsgCreator.ANY_PARTITION_API_VERSION, flags,
                              len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes

    def create_partition_key_msg(self, partition_key, topic, timestamp, \
                                 key_bytes, value_bytes):
        topic_bytes = bytes(topic)

        if len(topic_bytes) > DoryMsgCreator.getMaxTopicSize():
            raise DoryTopicTooLarge()

        msg_size = DoryMsgCreator.PARTITION_KEY_FIXED_BYTES + \
                   len(topic_bytes) + len(key_bytes) + len(value_bytes)

        if msg_size > DoryMsgCreator.getMaxMsgSize():
            raise DoryMsgTooLarge()

        buf = io.BytesIO()
        flags = 0
        buf.write(struct.pack('>ihhhih', msg_size,
                              DoryMsgCreator.PARTITION_KEY_API_KEY,
                              DoryMsgCreator.PARTITION_KEY_API_VERSION, flags,
                              partition_key, len(topic_bytes)))
        buf.write(topic_bytes)
        buf.write(struct.pack('>qi', timestamp, len(key_bytes)))
        buf.write(key_bytes)
        buf.write(struct.pack('>i', len(value_bytes)))
        buf.write(value_bytes)
        result_bytes = buf.getvalue()
        buf.close()
        return result_bytes

    def GetEpochMilliseconds(self):
        return int(time.time() * 1000)


