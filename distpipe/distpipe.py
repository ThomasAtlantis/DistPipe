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

class Node(threading.Thread):
    
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
        self.nodes: Dict[str, Node] = {}
        self.router = router
        self.role = router.role

    def connect(self, i_node: Node, o_node: Node):
        if i_node.role == o_node.role == self.role:
            q = queue.Queue(0)
            i_node.ostream.q.append(q)
            o_node.istream.q.append(q)
        elif i_node.role == self.role:
            i_node.ostream.q.append(DistQueue(o_node.name, self.router))
        elif o_node.role == self.role:            
            o_node.istream.q.append(DistQueue(o_node.name, self.router))

    def add(self, srcs: List[Node], tgt: Node):
        for src in srcs:
            self.connect(src, tgt)
        self.dependencies.append((srcs, tgt))
        self.nodes.update({tgt.name: tgt})
        self.nodes.update({src.name: src for src in srcs})
    
    def set_io(self, i_node: Node, o_node: Node):
        if i_node.role == "client":
            i_node.istream.q.append(queue.Queue(0))
        else:
            i_node.istream.q.append(DistQueue(i_node.name, self.router))
        
        if o_node.role == "client":
            o_node.ostream.q.append(queue.Queue(0))
        else:
            o_node.ostream.q.append(DistQueue(o_node.name, self.router))
        self.istream, self.ostream = i_node.istream, o_node.ostream

    def start(self):
        for name, node in self.nodes.items():
            self.router.register(name)
            if node.role == self.role:
                node.start()