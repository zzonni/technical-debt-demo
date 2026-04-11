
"""
- LegacyThreadPool: loaded at import but never used
- Very naive implementation without proper locking
"""

import threading, queue, time, random

class LegacyThreadPool:
    def __init__(self, size=2):
        self.q = queue.Queue()
        for _ in range(size):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()

    def worker(self):
        while True:
            fn, args = self.q.get()
            try:
                fn(*args)
            finally:
                self.q.task_done()

    def submit(self, fn, *args):
        self.q.put((fn, args))

# Do not create a global unused pool at import time. Initialize where needed.
