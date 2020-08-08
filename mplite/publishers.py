""" Provides publisher classes for publishing directly to the queue for the consumers """
import socket
import os
from time import time
from uuid import uuid4
from copy import deepcopy
from threading import Thread





from mplite.io import recv
from mplite.io import connection

from mplite.load_balancers import SimpleLoadBalancer





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


class ConnectionStack:

    def __init__(self, responses):
        self.conns = {}
        self.responses = responses


    def add_connection(self, conn_id, conn):
        self.conns[conn_id] = conn


    def await_results(self):
        while True:
            if len(self.responses.keys()) > 0:
                nextkey = list(self.responses.keys())[0]
                resp = self.responses[nextkey]
                del self.responses[nextkey]

                if nextkey in self.conns.keys():
                    self.conns[nextkey].send(str(resp).encode())
                    del self.conns[nextkey]




class SharedPublisher(ConnectionStack):

    """
    The SharedPublisher class is intended to be used for broadcasting messages / tasks to x amount of consumers.
    It is initialized before the worker processes are started and communicates to worker queues via TCP socket
    runs within the machines local network.

    """

    def __init__(self, q, index_, lock, resp):
        super().__init__(resp)
        self.queue = q
        self.index = index_
        self.lock = lock
        self.conn = connection(10000)




    def listen(self):
        """
        Listen for incoming messages from client connections to the task queue

            -- Upon receiving a task, a unique message id (uuid4) will be created and set as the task key in both
                the shared dictionary and shared index. The task queue contains the task "method" to map the input
                parameters to as well as the client connection. The input parameters are accessed under the "task"
                key and the client connection is accessed under the "conn" key
        :return:
        """
        print('Listening for Incoming Messages...')
        Thread(target=self.await_results, args=(), ).start()

        while True:

            try:
                conn, addr = self.conn.accept()
                data = recv(conn)
                if data:
                    msgid = str(uuid4())
                    self.index[msgid] = 100
                    self.queue[msgid] = data['task']
                    self.add_connection(msgid, conn)

            except OSError:  # Loop over OSError raised when using non-blocking sockets is not supported
                pass



