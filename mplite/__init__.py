from multiprocessing import (
    Process,
    Manager,
    Pipe,
    cpu_count,
)

from mplite.consumer import ConsumerObject
from mplite._publisher import SingleSocketPublisher




class StreamInstance(object):
    __slots__ = ['name', 'manager', 'index', 'queue']


    def __init__(self, name, manager):
        self.name = name
        self.manager = manager
        self.queue = manager.dict()
        self.index = manager.dict()


    def __setattr__(self, key, value):
        object.__setattr__(self, key, value)


    def __getattribute__(self, item):
        return object.__getattribute__(self, item)



class MPLite(object):
    _handler = None

    def __init__(self, handler_object):
        self.procs = []
        self.manager = Manager()
        self.pipes = {}
        self.shared_index = self.manager.dict()
        self.shared_queue = self.manager.dict()
        self.lock = self.manager.RLock()
        self._resolve(handler_object)


    def _resolve(self, handler_object):
        self._handler = handler_object

    @property
    def cores(self):
        return cpu_count()

    @property
    def handler(self):
        return self._handler

    @property
    def consumer(self):
        return ConsumerObject


    @property
    def publisher(self):
        return SingleSocketPublisher


    def _create_proc(self, target_obj):
        if issubclass(target_obj, self.consumer):
            procargs = (self.shared_queue, self.shared_index, self.lock, self.handler)
        else:
            procargs = (self.shared_queue, self.shared_index, self.lock)

        self.procs.append(Process(target=target_obj, args=procargs))
        return self.procs[-1]


    def _start(self):
        for p in self.procs:
            p.start()


    def _join(self):
        for p in self.procs:
            p.join()


    def run(self, consumers):
        if consumers >= self.cores:
            raise RuntimeWarning('MPLite: The number of consumers {%s} is >= to the number of cores {%s} on the machine'
                                 'This may impact performance especially for CPU bound constraints' %
                                 (str(consumers), str(self.cores)))

        for i in range(consumers):
            self._create_proc(self.consumer)
        self._create_proc(self.publisher)
        self._start()
        self._join()


def main(consumers=3):

    if __name__ == '__main__':
        pass

main()