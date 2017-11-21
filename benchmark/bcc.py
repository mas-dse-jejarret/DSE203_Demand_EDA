from __future__ import print_function

import datetime
import multiprocessing
import random
import time
from threading import current_thread
import numpy as np

from rx import Observable
from rx.concurrency import ThreadPoolScheduler

from rx import Observable, Observer

import requests

def make_connection(url):
    a = datetime.datetime.now()
    response = requests.get(url)
    if response.status_code == 200:
        b = datetime.datetime.now()

    return (b - a).microseconds / 1000.00

l = []

# calculate number of CPU's, then create a ThreadPoolScheduler with that number of threads
optimal_thread_count = multiprocessing.cpu_count()
optimal_thread_count = 3
pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

class Async(Observer):

    def __init__(self, id=1):
        self.id = id

    def on_next(self, value):
        print("Process {0}".format(self.id))
        t = make_connection("http://localhost/api/highest_monthly_sales_by_category/Education%20&%20Reference")
        l.append(t)

    def on_completed(self):
        print(np.max(l))
        # print("Done! {0}".format(self.id))

    def on_error(self, error):
        print("Error Occurred: {0}".format(error))


for i in range(4):
    # # Create Process 1
    Observable.range(1, 5) \
        .subscribe_on(pool_scheduler) \
        .subscribe(Async(id=i))

input("Press any key to exit\n")
