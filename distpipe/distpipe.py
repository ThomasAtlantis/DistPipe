import queue
import threading
from typing import Dict, List
from .transport import Router


class IOStream:

    def __init__(self):
        self.q = []

    def put(self, data):
        for q in self.q:
            q.put(data)

    def get(self):
        return [q.get() for q in self.q]

class Task(threading.Thread):
    
    def __init__(self, name, role='client'):
        super().__init__(name=name, daemon=True)
        self.istream = IOStream()
        self.ostream = IOStream()
        self.role = role
        self.name = name
    
    def run(self):
        while True:
            data = self.istream.get()
            if any(d is None for d in data):
                break
            data = self.process(data)
            self.ostream.put(data)

    def process(self, data):
        return data
    
class DistQueue:

    def __init__(self, name, router):
        self.name = name
        self.router = router

    def get(self):
        data = self.router.recv(self.name)
        return data

    def put(self, data):
        self.router.send(self.name, data)

class Pipe:

    def __init__(self, router: Router):
        self.dependencies = []
        self.tasks: Dict[str, Task] = {}
        self.router = router
        self.role = router.role

    def connect(self, i_task: Task, o_task: Task):
        if i_task.role == o_task.role == self.role:
            q = queue.Queue(0)
            i_task.ostream.q.append(q)
            o_task.istream.q.append(q)
        elif i_task.role == self.role:
            i_task.ostream.q.append(DistQueue(o_task.name, self.router))
        elif o_task.role == self.role:            
            o_task.istream.q.append(DistQueue(o_task.name, self.router))

    def add(self, srcs: List[Task], tgt: Task):
        for src in srcs:
            self.connect(src, tgt)
        self.dependencies.append((srcs, tgt))
        self.tasks.update({tgt.name: tgt})
        self.tasks.update({src.name: src for src in srcs})
    
    def set_io(self, i_task: Task, o_task: Task):
        if i_task.role == "client":
            i_task.istream.q.append(queue.Queue(0))
        else:
            i_task.istream.q.append(DistQueue(i_task.name, self.router))
        
        if o_task.role == "client":
            o_task.ostream.q.append(queue.Queue(0))
        else:
            o_task.ostream.q.append(DistQueue(o_task.name, self.router))
        self.istream, self.ostream = i_task.istream, o_task.ostream

    def start(self):
        for name, task in self.tasks.items():
            self.router.register(name)
            if task.role == self.role:
                task.start()