
"""
- LegacyThreadPool: loaded at import but never used
- Very naive implementation without proper locking
"""

import threading, queue, time, random
from typing import NoReturn

class LegacyThreadPool:
    def __init__(self, size: int = 2) -> None:
        self.q: queue.Queue = queue.Queue()
        for _ in range(size):
            t = threading.Thread(target=self.worker, daemon=True)
            t.start()

    def worker(self) -> NoReturn:
        while True:
            fn, args = self.q.get()
            try:
                fn(*args)
            finally:
                self.q.task_done()

    def submit(self, fn, *args) -> None:
        self.q.put((fn, args))

pool = LegacyThreadPool()
