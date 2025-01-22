from queue import Queue
from threading import Thread
from typing import Callable, Dict, Any, List, Type, TypeVar, Generic

T = TypeVar("T")


class TypedChannel(Generic[T]):
    """
    Канал для передачи данных между узлами
    """

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


class Node:
    """
    Узел обработки данных.
    """

    def __init__(self, name: str,
                 inputs: List[TypedChannel],
                 outputs: List[TypedChannel],
                 process_function: Callable[[Dict[str, Any]], Dict[str, Any]]):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.process_function = process_function

    def process(self):
        input_data = {channel.name: channel.receive() for channel in self.inputs}
        output_data = self.process_function(input_data)
        for channel in self.outputs:
            if channel.name in output_data:
                channel.send(output_data[channel.name])


class Pipeline:
    """
    Конвейер обработки данных.
    """

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

    def run(self, initial_data: Dict[str, Any]):
        # Заполнить начальные данные в каналы
        for channel_name, value in initial_data.items():
            if channel_name in self.channels:
                self.channels[channel_name].send(value)

        # Запустить обработку в узлах
        threads = [Thread(target=node.process) for node in self.nodes]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    # Пример использования

    def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"sum": data["input1"] + data["input2"]}


    def multiplier_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"product": data["sum"] * 2}

    pipeline = Pipeline(name="ExamplePipeline")

    input1 = pipeline.add_channel("input1", int)
    input2 = pipeline.add_channel("input2", int)
    sum_channel = pipeline.add_channel("sum", int)
    product_channel = pipeline.add_channel("product", int)

    summator = Node(
        name="Summator",
        inputs=[input1, input2],
        outputs=[sum_channel],
        process_function=summator_logic
    )

    multiplier = Node(
        name="Multiplier",
        inputs=[sum_channel],
        outputs=[product_channel],
        process_function=multiplier_logic
    )

    pipeline.add_node(summator)
    pipeline.add_node(multiplier)

    initial_data = {
        "input1": 2,
        "input2": 3,
    }

    pipeline.run(initial_data)

    result = product_channel.receive()
    print(f"Результат: {result}")
