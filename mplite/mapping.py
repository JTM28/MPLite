import os
import pickle
import inspect


class ObjectMapping:

    __instance__ = None

    def __init__(self):

        if ObjectMapping.__instance__ is None:
            ObjectMapping.__instance__ = self

        else:
            raise RuntimeError('The "ObjectMapping" class must only be created once')

        self.mapping = {}


    def add_func(self, func, kwargs: tuple):
        """
        Add a function to the mapping dict
        :param func: The method to be called
        :param kwargs: The keyword arguments to be passed to the function
        :return:
        """

        if func.__name__ not in self.mapping.keys():
            self.mapping[func.__name__] = {'func': func, 'kwargs': kwargs}
        else:
            raise KeyError



mapping = ObjectMapping()



def register(func):
    """
    Register a function to be added to the task mapping

    :param func: The function or classmethod to be called

    :returns The func method along with its arguments passed via dictionary
    """

    def _wrapper():
        try:
            args = inspect.signature(func)
            mapping.add_func(func, args)

        except KeyError:
            print('MPLiteException: RPC Method "%s" Already Exists' % str(func.__name__))

    return _wrapper()

