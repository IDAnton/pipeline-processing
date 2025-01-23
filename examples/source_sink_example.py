import random
from typing import Dict, Any
from pipeline.pipeline import Node, Pipeline, NodeType


def source() -> Dict[str, Any]:
    return {"input": [random.randint(1, 100) for _ in range(10)]}

def odd_filter(data: Dict[str, Any]) -> Dict[str, Any]:
    return {"output": [i for i in data["input"] if i % 2 != 0]}

def sink(data: Dict[str, Any]):
    with open("source_sink_output.txt", "w") as f:
        f.write(str(data))


pipeline = Pipeline(name="ExamplePipeline")

generator_output = pipeline.add_channel("input", list)
output = pipeline.add_channel("output", list)

generator_node = Node(
    name="source",
    outputs=[generator_output],
    process_function=source,
    node_type=NodeType.SOURCE
)

filter_node = Node(
    name="odd_filter",
    inputs=[generator_output],
    outputs=[output],
    process_function=odd_filter
)

output_node = Node(
    name="sink",
    inputs=[output],
    process_function=sink,
    node_type=NodeType.SINK
)

pipeline.add_node(generator_node)
pipeline.add_node(filter_node)
pipeline.add_node(output_node)

pipeline.run()
