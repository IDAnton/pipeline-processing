import random
from typing import Dict, Any
from pipeline.pipeline import Node, Pipeline, NodeType, PipelineNode


def create_internal_pipeline() -> Pipeline:
    def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"sum": data["input1"] + data["input2"]}
    def multiplier_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"output": data["sum"] * data["multiplier"]}

    internal_pipeline = Pipeline(name="internal_pipeline")
    input1 = internal_pipeline.add_channel("input1", int)
    input2 = internal_pipeline.add_channel("input2", int)
    input3 = internal_pipeline.add_channel("multiplier", int)
    sum_channel = internal_pipeline.add_channel("sum", int)
    product_channel = internal_pipeline.add_channel("output", int)
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
    internal_pipeline.add_node(summator)
    internal_pipeline.add_node(multiplier)
    return internal_pipeline

def source() -> Dict[str, Any]:
    return {"input1": random.randint(1, 10),
            "input2": random.randint(1, 10),
            "multiplier": 10,}

def sink(data: Dict[str, Any]):
    print(data)


pipeline = Pipeline("main pipeline")
input1 = pipeline.add_channel("input1", int)
input2 = pipeline.add_channel("input2", int)
input3 = pipeline.add_channel("multiplier", int)
output = pipeline.add_channel("output", int)

generator_node = Node(
    name="source",
    outputs=[input1, input2, input3],
    process_function=source,
    node_type=NodeType.SOURCE
)

internal_pipline = create_internal_pipeline()
pipeline_node = PipelineNode(name="PipelineNode",
                             inputs=[input1, input2, input3],
                             outputs=[output],
                             pipeline=internal_pipline)
output_node = Node(
    name="sink",
    inputs=[output],
    process_function=sink,
    node_type=NodeType.SINK
)
pipeline.add_node(generator_node)
pipeline.add_node(pipeline_node)
pipeline.add_node(output_node)
pipeline.run()

