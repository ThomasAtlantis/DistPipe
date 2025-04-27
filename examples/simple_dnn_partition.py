import sys

import torch; sys.path.append("..")
from distpipe.distpipe import Task, Pipe
from distpipe.transport import Router
import torch.nn as nn
import logging

logging.basicConfig(level=logging.INFO)

class HeadModel(nn.Module):
    def __init__(self):
        super().__init__()

        self.layers = nn.Sequential(
            nn.Linear(10, 10),
            nn.ReLU(),
            nn.Linear(10, 10),
            nn.ReLU()
        )

    def forward(self, x):
        x = self.layers(x)
        return x
    
class TailModel(nn.Module):
    def __init__(self):
        super().__init__()

        self.layers = nn.Sequential(
            nn.Linear(10, 5),
            nn.ReLU(),
            nn.Linear(5, 2)
        )

    def forward(self, x):
        x = self.layers(x)
        return x
    

class HeadModelTask(Task):

    def initialize(self):
        self.model = HeadModel()
    
    def process(self, data):
        with torch.inference_mode():
            return self.model(data[0])

class TailModelTask(Task):
    def initialize(self):
        self.model = TailModel()
    
    def process(self, data):
        with torch.inference_mode():
            return self.model(data[0])
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--node_map', type=str, help='Path to the node map JSON file')
    args = parser.parse_args()

    head_model = HeadModelTask('head_model', role='client')
    tail_model = TailModelTask('tail_model', role='server')
    router = Router.from_json(args.node_map)

    if router.role == "client":
        head_model.initialize()
    else:
        tail_model.initialize()

    pipe = Pipe(router=router)
    pipe.add(srcs=[head_model], tgt=tail_model)
    pipe.set_io(head_model, tail_model)
    pipe.start()

    if pipe.role == "client":
        pipe.istream.put(torch.randn(1, 10))
        print(pipe.ostream.get()[0])
        pipe.istream.put(torch.randn(1, 10))
        print(pipe.ostream.get()[0])
        router.shutdown()