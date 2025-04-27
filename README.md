# DistPipe

DistPipe is a distributed framework to implement device-cloud collaborative workflow.

## Usage

### Step 0: Define connection
Specify the network topology using a JSON file:
```json
{
    "client_addr": ["192.168.1.126", 6000],
    "server_addr": ["192.168.1.126", 6001],
    "role": "server"
}
```
Here, the field `role` indicates the current platform (`server` or `client`). Next, define a Router to connect the device and the cloud.

```python
from distpipe.transport import Router

router = Router.from_json("node_map.client.json")
```

### Step 1: Define custom tasks
```python
from distpipe.distpipe import Task

class Identical(Task):

    def process(self, data):
        return data[0]

class Log(Task):

    def process(self, data):
        return data[0] + 1

class Add(Task):

    def process(self, data):
        return data[0] + data[1]

identical = Identical('identical', role='client')
log = Log('log', role='client')
add = Add('add', role='server')
```

### Step 2: Define pipeline as DAG
```python
from distpipe.distpipe import Pipe

pipe = Pipe(router=router)
pipe.add(srcs=[identical], tgt=log)
pipe.add(srcs=[identical, log], tgt=add)
pipe.set_io(identical, add)
pipe.start()
```

### Step 3: Launch the pipeline

```python
if router.role == "client":
    pipe.istream.put(1)
    print(pipe.ostream.get()[0])
    pipe.istream.put(2)
    print(pipe.ostream.get()[0])
```

### Optional: Shutdown the system
```python
router.shutdown()
```