from timeit import default_timer
from random import choice
from time import time


class Timer(object):
    def __init__(self, functions, data, max_run_size, min_run_size=2, scale=2):
        self.functions = functions
        self.data = data
        self.scale = scale
        self.max_run_size = max_run_size
        self.min_run_size = min_run_size

    def bootstrap(self):
        while True:
            yield choice(self.data)

    def run(self):
        strap = self.bootstrap()
        size = self.min_run_size
        print('-'*64)
        print('|{:^30}|{:^15}|{:^15}|'.format("Function", "Size", "Time"))
        print('-'*64)
        while size <= self.max_run_size:
            sample = [next(strap) for _ in range(size)]
            for key, value in self.functions.items():
                t0 = time()
                value(self.data)
                print('|{:<30}|{:>15,d}|{:>15,.3f}|'.format(key, size, (time() - t0)))
            size *= self.scale
        print('-'*64)
