from queue import Queue
from threading import Thread
from typing import Callable, Dict, Any, List, Type, TypeVar, Generic
from enum import Enum


T = TypeVar("T")


class TypedChannel(Generic[T]):
    def __init__(self, name: str, data_type: Type[T]):
        self.name = name
        self.data_type = data_type
        self.queue = Queue()

    def send(self, data: T):
        if not isinstance(data, self.data_type):
            raise TypeError(
                f"Channel '{self.name}' expects data of type {self.data_type.__name__}, "
                f"but got {type(data).__name__}."
            )
        self.queue.put(data)

    def receive(self) -> T:
        return self.queue.get()

    def is_empty(self) -> bool:
        return self.queue.empty()


class NodeType(Enum):
    DEFAULT = "default"
    SOURCE = "source"
    SINK = "sink"


class Node:
    def __init__(self, name: str,
                 process_function,
                 node_type: NodeType = NodeType.DEFAULT,
                 inputs: List[TypedChannel] = None,
                 outputs: List[TypedChannel] = None):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.process_function = process_function
        self.node_type = node_type

    def process(self):
        if self.node_type == NodeType.DEFAULT:
            input_data = {channel.name: channel.receive() for channel in self.inputs}
            output_data = self.process_function(input_data)
            for channel in self.outputs:
                if channel.name in output_data:
                    channel.send(output_data[channel.name])
        if self.node_type == NodeType.SOURCE:
            output_data = self.process_function()
            for channel in self.outputs:
                if channel.name in output_data:
                    channel.send(output_data[channel.name])
        if self.node_type == NodeType.SINK:
            input_data = {channel.name: channel.receive() for channel in self.inputs}
            self.process_function(input_data)

class Pipeline:
    def __init__(self, name: str):
        self.name = name
        self.nodes: List[Node] = []
        self.channels: Dict[str, TypedChannel] = {}

    def add_channel(self, channel_name: str, data_type: Type[T]) -> TypedChannel[T]:
        channel = TypedChannel(channel_name, data_type)
        self.channels[channel_name] = channel
        return channel

    def add_node(self, node: Node):
        self.nodes.append(node)

    def run(self, initial_data: Dict[str, Any] = None):
        if initial_data is not None:
            for channel_name, value in initial_data.items():
                if channel_name in self.channels:
                    self.channels[channel_name].send(value)

        threads = [Thread(target=node.process) for node in self.nodes]
        for thread in threads:
            thread.start()

if __name__ == "__main__":
    def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"sum": data["input1"] + data["input2"]}

    def multiplier_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"product": data["sum"] * data["multiplier"]}

    pipeline = Pipeline(name="ExamplePipeline")

    input1 = pipeline.add_channel("input1", int)
    input2 = pipeline.add_channel("input2", int)
    input3 = pipeline.add_channel("multiplier", float)
    sum_channel = pipeline.add_channel("sum", int)
    product_channel = pipeline.add_channel("product", float)

    summator = Node(
        name="Summator",
        inputs=[input1, input2],
        outputs=[sum_channel],
        process_function=summator_logic
    )

    multiplier = Node(
        name="Multiplier",
        inputs=[sum_channel, input3],
        outputs=[product_channel],
        process_function=multiplier_logic
    )

    pipeline.add_node(summator)
    pipeline.add_node(multiplier)

    initial_data = {
        "input1": 2,
        "input2": 3,
        "multiplier": 0.5,
    }

    pipeline.run(initial_data)

    result = product_channel.receive()
    print(f"Result: {result}")
