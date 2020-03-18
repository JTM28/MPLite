import numpy as np
from time import time
from collections import deque



def roc(x1, x2, n):
    return n / (x2 - x1)


class SimpleLoadBalancer(object):
    """
    Basic LB class for managing the publish rate to consumers
    """
    __slots__ = ['_max_stack', '_queue_health', '_window', 'xstack', 'size', 'roc']

    def __init__(self):
        self.mxstack = 1e5
        self.queue_health = 0
        self.window = 100         # The rolling lookback window for assessing the state of the queue
        self.xstack = deque()     # Holds the time stack of time.time() appended on each call
        self.roc = deque()        # The stack for holding the current ROC *value returned by diff*
        self.size = 0             # The current size of the multiprocessing.JoinableQueue().qsize()

    @property
    def mxstack(self):
        """ Sets the max size of collections.dequeue stacks """
        return self._max_stack

    @property
    def queue_health(self):
        if isinstance(self._queue_health, ValueError):
            return 0
        return self._queue_health

    @property
    def window(self):
        return self._window

    @mxstack.setter
    def mxstack(self, n):
        if isinstance(n, (int, float)):
            if n > 1e6:
                raise Exception('Max Stack Limit Set at 1e6 (1,000,000)')
            self._max_stack = n

    @queue_health.setter
    def queue_health(self, val):
        if isinstance(val, (int, float)):
            self._queue_health = val
        else:
            self._queue_health = ValueError

    @window.setter
    def window(self, n):
        if not isinstance(n, (int, float)):
            raise Exception('LoadBalancer: The propery "window" must be of instance (int, float)')
        self._window = n


    def _pop(self):
        try:
            self.xstack.popleft()

            if len(self.roc) > self.mxstack:
                self.roc.popleft()

        except IndexError:
            print('LoadBalancer: LPOP attempted on empty stack')


    def _stack(self, n):
        if len(self.xstack) > self.mxstack:
            self._pop()
        self.xstack.append(time())
        self.size = n

    def on_publish(self, n: int):
        """
        Called when publisher sends a message
        :param n: The size of the queue *multiprocessing.JoinableQueue().qsize()*

        """
        self._stack(n)
        if len(self.xstack) > self.window:
            self.roc.append(roc(self.xstack[-self.window], self.xstack[-1], self.size))
        if len(self.roc) > 10:
            print(np.mean(self.roc))