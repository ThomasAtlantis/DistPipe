import sys; sys.path.append("..")
from distpipe.distpipe import Task, Pipe
from distpipe.transport import Router
import logging

logging.basicConfig(level=logging.INFO)

class Layer(Task):

    def process(self, data):
        print(f"{data[0]} + 1 -> {data[0] + 1}")
        return data[0] + 1

class DLayer(Task):

    def process(self, data):
        print(f"{data[0]} + {data[1]} -> {data[0] + data[1]}")
        return data[0] + data[1]

class A():

    def __init__(self):
        self.layer_1 = Layer('layer_1', role='client')
        self.layer_2 = Layer('layer_2', role='client')
        self.layer_3 = DLayer('layer_3', role='client')
        self.layer_4 = Layer('layer_4', role='server')
        self.layer_5 = Layer('layer_5', role='server')
    
    def pipeline(self, router):
        pipe = Pipe(router=router)
        pipe.add(srcs=[self.layer_1], tgt=self.layer_2)
        pipe.add(srcs=[self.layer_1, self.layer_2], tgt=self.layer_3)
        pipe.add(srcs=[self.layer_3], tgt=self.layer_4)
        pipe.add(srcs=[self.layer_4], tgt=self.layer_5)
        pipe.set_io(self.layer_1, self.layer_5)
        pipe.start()
        return pipe

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--node_map', type=str, help='Path to the node map JSON file')
    args = parser.parse_args()

    a_instance = A()
    router = Router.from_json(args.node_map)
    pipe = a_instance.pipeline(router)

    if pipe.role == "client":
        pipe.visualize()
        pipe.istream.put(1)
        print(pipe.ostream.get()[0])
        pipe.istream.put(2)
        print(pipe.ostream.get()[0])
        router.shutdown()