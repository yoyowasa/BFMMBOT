import multiprocessing
import sys

def f():
    print('child sys.executable', sys.executable)
    print('child base', getattr(sys, '_base_executable', None))
    print('child prefix', sys.prefix, sys.base_prefix)

if __name__ == '__main__':
    p = multiprocessing.Process(target=f)
    p.start()
    p.join()
