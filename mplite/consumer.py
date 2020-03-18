import os
from collections import OrderedDict


def sort_dict_by_value(dict_obj, reverse=True):
    return OrderedDict(sorted(dict_obj.items(), key=lambda x: x[1], reverse=reverse))


class ConsumerObject(object):
    """
    Class providing the functions for consuming and processing messages and tasks published to the queue
    """
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
        self.consumer = shared_queue
        self.shared_index = shared_index
        self.sorted_queue = []
        self.lock = lock
        self.handler = handler
        self.__call__()

    def __call__(self):
        self.handler = self.handler()   # Initialize the handler class
        self.get()


    def _get(self):
        self.lock.acquire(blocking=True)
        try:
            key = list(sort_dict_by_value(self.shared_index).keys())[0]
            msg = self.consumer[key]
            del self.shared_index[key]
            del self.consumer[key]
        finally:
            self.lock.release()
            if 'msg' in locals().keys():
                return locals()['msg']
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
                    self.handler.on_message(msg)

    def multithread(self):
        from threading import Thread
        for _ in range(2):
            print('MPLiteBackend: Starting Thread %s On [PID- %s]' % (str(_), os.getpid()))
            t = Thread(target=self.get, args=(), )
            t.start()
