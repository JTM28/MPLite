""" Provides publisher classes for publishing directly to the queue for the consumers """
import socket
import ast
import struct
from time import time
from uuid import uuid4
from threading import Thread
from typing import Union

from mplite.load_balancers import SimpleLoadBalancer


def _recvall(conn: socket.socket, n: int) -> Union[bytes, None]:
    """
    Helper Function to ensure the entire message is received from a socket connection

    :param conn: The client connection accepted via socket.accept
    :param n: The size of the message in bytes
    :return: Message in bytes if not None else None
    """
    data = bytearray()
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data



def recv(conn) -> Union[dict, str, Exception]:
    """
    :param conn: The client socket connection
    :return:
    """
    rawsize = _recvall(conn, 4)  # Retrieve the Message Size
    size = struct.unpack('>I', rawsize)[0]  # Unpack Message (Big-Endian Unsigned Int)
    msg = _recvall(conn, size)
    try:
        # Try to Evaluate Message as Dict
        return ast.literal_eval(msg.decode('utf-8'))

    except SyntaxError:
        return str(msg.decode('utf-8'))

    except Exception as e:
        print(e)



class ThreadFrame(object):
    _ping_interval = 10
    _keep_alive = False

    def __init__(self, conn):
        self.__call__(conn)

    def __call__(self, conn):
        msg = recv(conn)
        print(msg)
        if isinstance(msg, dict):
            self.on_task(msg)

        elif isinstance(msg, str):
            if msg == 'keepalive':
                self._keep_alive = True
                self._set_ping()

    def _set_ping(self):
        self.ping = time()

    def on_task(self, msg):
        print(msg)




class _ManageConsumers(object):
    """
    Internal class for managing the pids of all consumers.
    """
    ALL_PIDS_RECV = False
    THREADED = False

    def __init__(self):
        self.count = 0
        self.pipes = []
        self.responses = {}
        self.pids = {}

    def add_pipe(self, pipe):
        self.pipes.append(pipe)
        pipe.send({'send': 'pid', 'ident': self.count})
        self.count += 1

    def await_ready(self):
        c = 0
        while self.ALL_PIDS_RECV is False:
            for p in self.pipes:
                data = p.recv()
                if data:
                    self.pids[str(data['pid'])] = {'pipe': self.pipes[data['ident']], 'last-response': time()}
                    c += 1
            if c == len(self.pids):
                self.ALL_PIDS_RECV = True
                return


    def await_responses(self):
        while True:
            for pid in self.pids.keys():
                data = self.pids[pid]['pipe'].recv()
                if data:
                    if str(pid) == str(data):
                        self.pids[str(data)]['last-response'] = time()


class SingleSocketPublisher(SimpleLoadBalancer):
    """
    Single publisher instance for publishing messages / tasks for all consumer instances process
    """
    SEMAPHORE = None
    ADDR = ('0.0.0.0', 43132)
    _MANAGER = _ManageConsumers()


    def __init__(self,
                 shared_queue,
                 shared_index,
                 lock,
                 thread_connections: bool = False,
                 use_daemon_threads: bool = True,
                 manage_pids: bool = False):
        super().__init__()
        self._run_as_threaded = thread_connections
        self._run_threads_as_background = use_daemon_threads
        self.shared_queue = shared_queue
        self.shared_index = shared_index
        self.threads = []
        self.pids = []
        self.max_priority = 0
        self.lock = lock
        self.bad_msg = []
        self._manage_pids = manage_pids
        self.__call__()


    def __call__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.ADDR)
        self.sock.listen(10000)
        self.publish()


    def _add_pid(self, p):
        print('Publisher Adding PID: [%s]' % str(p))
        self.pids.append(int(p))

    def _create_thread(self, conn):
        t = Thread(target=ThreadFrame, args=(conn, ))
        self.threads.append(t)
        if self._run_threads_as_background is True:
            t.setDaemon = True
        t.start()

    def _broadcast_alert(self, uid):
        if self._MANAGER.THREADED is False:
            self.shared_index[uid] = self.max_priority + 1
            self.shared_queue[uid] = {'publisher-internal': 'thread'}
            self._MANAGER.THREADED = True

    def publish(self):
        print('MPLiteBackend: Publisher Ready')
        while True:
            try:
                conn, _ = self.sock.accept()
                if self._run_as_threaded is True:
                    self._create_thread(conn)

                else:
                    msg = recv(conn)
                    if msg is not None:
                        """ 
                        Publisher Does Not Need to Acquire the multiprocessing.RLock Object
                        --------------------------------------------------------------
                            1. Create a unique ID for each Message
                            2. Add {<unique-id>: msg['task']} to the shared queue
                            3. Add {<unique-id>: msg['priority']} to shared index
                        ---------------------------------------------------------------        
                         """
                        uid = str(uuid4())
                        self.shared_queue[uid] = msg['task']
                        self.shared_index[uid] = msg['priority']
                        if msg['priority'] > self.max_priority:
                            self.max_priority = msg['priority']

                        if len(list(self.shared_index.keys())) > 50:
                            self._broadcast_alert(str(uuid4()))


            except OSError:
                pass
