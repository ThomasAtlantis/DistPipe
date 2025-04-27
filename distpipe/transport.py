import json
import logging
import queue
import socket
import struct
import threading
import time
import hashlib
from .message import Message



class Receiver(threading.Thread):
    def __init__(self, skt: socket.socket, queue: queue.Queue):
        threading.Thread.__init__(self, name=__class__.__name__)
        self.logger = logging.getLogger(__name__)
        self.queue = queue
        self.skt = skt
        self.skt.listen(1)

        self.header_size = struct.calcsize(Message.header)
        self.conn = None

    def close(self):
        if self.skt:
            self.skt.close()
        if self.conn:
            self.conn.close()
    
    def run(self):
        self.conn, addr = self.skt.accept()
        self.logger.info(f"Connection accepted from {addr}")
        while True:
            try:
                header = self.conn.recv(self.header_size, socket.MSG_WAITALL)
                mtype, body_len = struct.unpack(Message.header, header)
                mbody = self.conn.recv(body_len, socket.MSG_WAITALL)
                mtype = mtype.decode('utf-8')
                mbody = Message.unpack(mbody)
            except Exception:
                break
            self.logger.debug(f"Received ({mtype}, {mbody})")
            self.queue.put((mtype, mbody))
        self.logger.debug("Connection closed.")
        if self.conn: self.conn.close()


class Sender(threading.Thread):

    def __init__(self, skt: socket.socket, queue: queue.Queue):
        threading.Thread.__init__(self, name=__class__.__name__)
        self.logger = logging.getLogger(__name__)
        self.skt = skt
        self.queue = queue

    def run(self):
        while True:
            mtype, mbody = self.queue.get()
            if mtype is None:
                break
            self.logger.debug(f"Sending({mtype}, {mbody})")
            mtype = hashlib.md5(mtype.encode('utf-8')).hexdigest()
            data, body_len = Message.pack(mtype, mbody)
            self.skt.sendall(data)
        self.logger.info("Sender shutdown")

class Router:
    
    def __init__(self, client_addr, server_addr, role='client'):
        self.role = role
        self.logger = logging.getLogger(__name__)
        self.local_addr, self.remote_addr = client_addr, server_addr
        if role != 'client': self.local_addr, self.remote_addr = server_addr, client_addr
        self.init_receiver()
        self.logger.info("Receiver initialized.")
        self.init_sender()
        self.logger.info("Sender initialized.")
        self.queues = {}
        self.stop_dispatcher = threading.Event()
        self.dispatcher = threading.Thread(
            target=self.dispatch, name="DispatcherThread")
        self.dispatcher.start()

    @classmethod
    def from_json(cls, path):
        with open(path, 'r') as f:
            params = json.load(f)
        return cls(**params)
    
    def init_receiver(self, timeout=3, poll=True):
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                skt.bind(tuple(self.local_addr))
                break
            except OSError:
                if not poll: break
                time.sleep(timeout)
        self.receiver = Receiver(skt, queue.Queue(0))
        self.receiver.start()
        
    def init_sender(self, timeout=3, poll=True):
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                self.logger.info("Trying to connect to the remote node...")
                skt.connect(tuple(self.remote_addr))
                break
            except (ConnectionRefusedError, OSError) as e:
                if not poll: break
                if isinstance(e, OSError):
                    self.logger.error(e)
                    skt.close()
                    skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                time.sleep(timeout)
        self.sender = Sender(skt, queue.Queue(0))
        self.sender.start()
    
    def dispatch(self):
        while not self.stop_dispatcher.is_set():
            if not self.receiver.queue.empty():
                mtype, _ = self.receiver.queue.queue[0]
                if mtype in self.queues:
                    mtype, mbody = self.receiver.queue.get()
                    self.queues[mtype].put(mbody)

    def register(self, name):
        mtype = hashlib.md5(name.encode('utf-8')).hexdigest()
        self.queues[mtype] = queue.Queue(0)

    def send(self, mtype: str, mbody: object):
        self.sender.queue.put((mtype, mbody))

    def recv(self, mtype: str):
        mtype = hashlib.md5(mtype.encode('utf-8')).hexdigest()
        return self.queues[mtype].get()
    
    def shutdown(self):
        self.sender.queue.put((None, None))
        self.receiver.close()
        self.stop_dispatcher.set()