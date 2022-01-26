
import threading
import multiprocessing


def countdown():
    x = 100000000
    while x > 0:
        x -= 1


# Implementation with multithreading
def implementation_1():
    thread_1 = threading.Thread(target= countdown)
    thread_2 = threading.Thread(target=countdown)
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()


def implementation_2():
    countdown()
    countdown()


def implementation_3():
    proc_1 = multiprocessing.Process(target=countdown)
    proc_2 = multiprocessing.Process(target=countdown)
    proc_1.start()
    proc_2.start()
    proc_1.join()
    proc_2.join()

