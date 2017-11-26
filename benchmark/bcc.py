from __future__ import print_function

import datetime
import multiprocessing
import random
import time
from threading import current_thread
import numpy as np
import requests


from rx import Observable
from rx.concurrency import ThreadPoolScheduler

from rx import Observable, Observer

import requests

def printthread(val):
    print("{}, thread: {}".format(val, current_thread().name))

def make_connection(url):
    a = datetime.datetime.now()
    response = requests.get(url)
    if response.status_code == 200:
        b = datetime.datetime.now()

    return (b - a).microseconds / 1000.00

l = []

# calculate number of CPU's, then create a ThreadPoolScheduler with that number of threads
optimal_thread_count = multiprocessing.cpu_count()
optimal_thread_count = 20
pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

class Async(Observer):

    def __init__(self, id=1):
        self.id = id

    def on_next(self, value):
        print("Process {0}".format(self.id))
        #t = make_connection("http://localhost/api/highest_monthly_sales_by_category/Education%20&%20Reference")
        t = make_connection("http://localhost/api/web_method/json?c1=productid&c2=shipdate&c3=unitprice")
        pct = (t/1000.0) * 100
        print("{0}:{1}".format(t, pct))
        l.append(t)
        c = requests.get('http://localhost:8080/save/{0}/{1}'.format(1, pct)).content
        print(c)
    def on_completed(self):
        print(np.max(l))
        # print("Done! {0}".format(self.id))

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))


for i in range(10):
    # # Create Process 1
    Observable.range(1, 10) \
        .subscribe_on(pool_scheduler) \
        .subscribe(Async(id=i))

#input("Press any key to exit\n")

printthread("\nAll done")
pool_scheduler.executor.shutdown()