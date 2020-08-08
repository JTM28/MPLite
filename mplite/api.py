import socket
import struct


class BackendBaseException(Exception):
    """ Base Exception Class for Backend API """


class BackendConnectionError(BackendBaseException):
    """ Connection to the Backend local server could not be initiated """


def publish(task, priority: int = 0):
    """
    :param task: (str, dict) -> The task being sent to pool of consumers that will be passed to the on_message
                                method of the Handler Class.
    :param priority: -> Default is 0. If value different then default, will be processed by consumers using the
                        priority based algorithm. The value of priority can either be any int value > 0 or -1. The
                        value of any single priorty is arbitrary in that the highest valued tasks will be processed
                        first. A priority of -1 will be set to the max priority value of the current tasks within
                        the queue.

                        :Example:
                        tasks = []
                        1. for i in range(5):
                        2.    tasks.append({'task': {}, 'priority': int(i)})
                        3. tasks.append({'task': {}, 'priority': int(100)})
                        4. tasks.append({'task': {}, 'priority': int(1000)})
                        5. tasks.append({'task': {}, 'priority': int(-1)})

                        # Assume all the above tasks are waiting to be processed within the queue
                        # The above tasks will be processed in the following order:
                            1 -> Task on Line 4
                            2 -> Task on Line 5
                            3 -> Task on Line 3
                            4 - > Tasks appended in for loop

                        # The reason the line 4 task is processed before line 5 task is due to the fact that while they
                        # both have the same priority value the line 4 task was added to the queue before the line 5
                        # task and the order of processing for multiple tasks with same priority is by the timestamp
                        # that each task was added to the queue

    :return:
    """
    if not isinstance(priority, int):
        raise ValueError('MPLite: The send_task method arg "priority" must be of type int')

    # Ensure any negative int < -1 is set to -1 to avoid confusion for consumers
    if priority < -1: priority = -1

    def _pack_and_send(conn: socket.socket, data: str.encode):
        """
        :param conn: The socket connection
        :param data: The data as a string UTF-8 Encoded
        """
        # Pack Data W/ Big-Endian Unsigned Int (Standard Byte Size 4)
        packed = struct.pack('>I', len(data)) + data
        conn.sendall(packed)
        data = conn.recv(1024)
        if data:
            print(data)

        else:
            conn.close()

    try:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('127.0.0.1', 10000))
            msg = {'task': task, 'priority': priority}
            _pack_and_send(s, str(msg).encode('utf-8'))

        except ConnectionError:
            raise BackendConnectionError

    except BackendConnectionError:
        print('mplite backend not connected')


from threading import Thread

for _ in range(100, 199):
    Thread(target=publish, args=({"task": '%s' % str(_)}, 5)).start()