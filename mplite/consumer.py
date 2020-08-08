import os
from collections import OrderedDict



def sort_dict_by_value(dict_obj, reverse=True):
    return OrderedDict(sorted(dict_obj.items(), key=lambda x: x[1], reverse=reverse))



class Consumer:

    def __init__(self, q, index_, lock, responses):
        self.queue = q
        self.index = index_
        self.lock = lock
        self.responses = responses



    def _receive(self):
        self.lock.acquire()
        try:
            key = list(sort_dict_by_value(self.index).keys())[0]
            msg = self.queue[key]
            del self.index[key]
            del self.queue[key]

        except KeyError:
            pass

        finally:
            self.lock.release()
            if 'msg' in locals().keys():
                return locals()['msg'], locals()['key']


    def consume(self):
        """
        Consume messages pushed by the publisher. Calls private _receive message which ensures the proper locking
        takes place to avoid memory corruption across processes

        :returns Runs indefinetly as an event loop
        """
        print('Consumer Awaiting Tasks...')

        while True:
            try:
                msg = self._receive()
                if isinstance(msg, tuple):
                    msg, key = msg
                    self.responses[key] = key

            except (AttributeError, IndexError):
                pass




