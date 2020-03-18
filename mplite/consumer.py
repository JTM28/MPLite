import os
from time import sleep
from collections import OrderedDict


def sort_dict_by_value(dict_obj, reverse=True):
    return OrderedDict(sorted(dict_obj.items(), key=lambda x: x[1], reverse=reverse))


class ConsumerObject(object):
    _internal_key = 'publisher-internal'
    _consumer_obj = None
    _is_threaded  = False
    _pid_sent     = False

    def __init__(self, shared_queue, shared_index, lock, handler):
        super().__init__()
        """
        :param ipc: The interprocess comm object used to distribute tasks.
            --------
            Options:
            --------
            Queue => Use multiprocessing.JoinableQueue
            Pipe => Use multiprocessing.Pipe(duplex=True) for bidirectional comm between MAX(2) processes

        :param manage_pids: If true will halt on startup til all consumers have reported their PIDs to the publisher
        """
        self._use_socket_calls = False
        self._max_consumers = 0
        self.consumer = shared_queue
        self.shared_index = shared_index
        self.sorted_queue = []
        self.lock = lock
        self.handler = handler()
        self.get()

    def __call__(self):
        while self._pid_sent is False:
            data = self.pipe.recv()
            if data:
                if isinstance(data, dict) and 'send' in data.keys():
                    self.pipe.send({'pid': os.getpid(), 'ident': data['ident']})
                    self._pid_sent = True
        else:
            self.get()


    @property
    def consumer_limit(self):
        return self._max_consumers

    @property
    def consumer(self):
        return self._consumer_obj

    @consumer_limit.setter
    def consumer_limit(self, n):
        if isinstance(n, int):
            self._max_consumers = n

    @consumer.setter
    def consumer(self, obj):
        self._consumer_obj = obj

    def _internal_handler(self, cmd):
        if cmd == 'thread':
            if self._is_threaded is False:
                print('Threading')
                self.multithread()
            self._is_threaded = True


    def _get(self):
        self.lock.acquire(blocking=True)
        try:
            key = list(sort_dict_by_value(self.shared_index).keys())[0]
            task = self.consumer[key]
            del self.shared_index[key]
            del self.consumer[key]
        finally:
            self.lock.release()
            if 'task' in locals().keys():
                return task
            return


    def get(self):
        print('MPLiteBackend: Consumer [PID- %s] Ready' % os.getpid())
        while True:
            if self._use_socket_calls is False:
                """
                Steps:
                    1. Wait for the RLock object to be released by the current consumer accessing its resources
                    2. Acquire the non-blocking RLock in the current process
                    3. Sort the shared index by value aka priority and retrieve the top id from the reversed stack
                    4. Map the id to the shared_queue containing the task
                    5. Delete the id and its respective value from both the shared index and shared queue
                """
                msg = self._get()
                if msg is not None:
                    print(msg)
                    self.handler.on_message(msg)

    def multithread(self):
        from threading import Thread
        for _ in range(2):
            print('MPLiteBackend: Starting Thread %s On [PID- %s]' % (str(_), os.getpid()))
            t = Thread(target=self.get, args=(), )
            t.start()
