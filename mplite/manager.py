import multiprocessing.reduction as mpr
from multiprocessing import Manager
from multiprocessing import Process
from multiprocessing import cpu_count

from mplite.consumer import Consumer
from mplite.publishers import SharedPublisher



class SharedManager:

    def __init__(self, port=1000):
        self.port = port
        self.procs = []
        self.manager = Manager()
        self.task_queue = self.manager.dict()
        self.priority_index = self.manager.dict()
        self.responses = self.manager.dict()

        self.lock = self.manager.Lock()   # Ensure exclusive locking to avoid message corruption


    @staticmethod
    def cores() -> cpu_count:
        """ Return the number of cores on the machine - 1 to ensure the SharedManager has its own core """
        return cpu_count() - 3


    @property
    def publisher(self) -> SharedPublisher:
        """ :returns A new instance of the SharedPublisher class """

        return SharedPublisher(self.task_queue, self.priority_index, self.lock, self.responses)


    @property
    def consumer(self) -> Consumer:
        """ :returns A new instance of the Consumer class """
        return Consumer(self.task_queue, self.priority_index, self.lock, self.responses)




    def build(self):
        self.procs.append(Process(target=self.publisher.listen, args=()))

        for _ in range(self.cores()):
            self.procs.append(Process(target=self.consumer.consume, args=()))


        for p in self.procs:
            p.start()

        for p in self.procs:
            p.join()



if __name__ == '__main__':
    sm = SharedManager()
    sm.build()






