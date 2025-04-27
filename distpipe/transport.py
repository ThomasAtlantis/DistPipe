import json
import logging
import queue
import socket
import struct
import threading
import time
from .message import Message


class Receiver(threading.Thread):
    def __init__(self, skt: socket.socket, queue: queue.Queue):
        threading.Thread.__init__(self, name=__class__.__name__)
        self.logger = logging.getLogger(__name__)
        self.queue = queue
        self.skt = skt
        self.skt.listen(1)

        self.header_size = Message.header_size()
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
                mtype = Message.Type(mtype)
                mbody = self.conn.recv(body_len, socket.MSG_WAITALL)
                mbody = Message.unpack(mbody)
            except Exception:
                break
            self.logger.debug(f"Received ({mtype.name}, {mbody})")
            self.queue.put((mtype, mbody))
            if mtype == Message.Type.SHUTDOWN:
                break
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
            self.logger.debug(f"Sending({mtype.name}, {mbody})")
            data, body_len = Message.pack(mtype, mbody)
            self.skt.sendall(data)
            if mtype == Message.Type.SHUTDOWN:
                break
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
                self.logger.info("Failed to bind socket, retrying...")
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
                mtype, mbody = self.receiver.queue.queue[0]
                if mtype == Message.Type.SHUTDOWN:
                    break
                if mtype == Message.Type.PIPELINE and mbody[0] in self.queues:
                    self.receiver.queue.get()
                    self.queues[mbody[0]].put(mbody[1])
        self.logger.info("Dispatcher shutdown")
        self.shutdown()

    def register(self, name):
        self.queues[name] = queue.Queue(0)

    def send(self, name: str, mbody: object):
        self.sender.queue.put((Message.Type.PIPELINE, (name, mbody)))

    def recv(self, name: str):
        return self.queues[name].get()
    
    def shutdown(self):
        self.sender.queue.put((Message.Type.SHUTDOWN, None))
        self.stop_dispatcher.set()
        exit(0)