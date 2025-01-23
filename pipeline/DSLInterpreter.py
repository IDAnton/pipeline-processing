import yaml
from typing import Callable, Dict, Any, List, Type
from pipeline import Pipeline, Node


class DSLPipelineInterpreter:
    def __init__(self, dsl_definition: str, logic_registry: Dict[str, Callable]):
        self.dsl = yaml.safe_load(dsl_definition)
        self.logic_registry = logic_registry

    def interpret(self):
        pipeline_name = self.dsl["pipeline"]["name"]
        pipeline = Pipeline(name=pipeline_name)

        # Создаем каналы
        channels = {}
        for channel_def in self.dsl["pipeline"]["channels"]:
            channel_name = channel_def["name"]
            channel_type = eval(channel_def["type"])
            channels[channel_name] = pipeline.add_channel(channel_name, channel_type)

        # Создаем узлы
        for node_def in self.dsl["pipeline"]["nodes"]:
            node_name = node_def["name"]
            inputs = [channels[ch] for ch in node_def["inputs"]]
            outputs = [channels[ch] for ch in node_def["outputs"]]
            logic = self.logic_registry[node_def["logic"]]
            pipeline.add_node(Node(name=node_name, inputs=inputs, outputs=outputs, process_function=logic))

        initial_data = self.dsl["pipeline"]["initial_data"]
        return pipeline, initial_data

if __name__ == "__main__":
    def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"sum": data["input1"] + data["input2"]}


    def multiplier_logic(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"product": data["sum"] * 2}


    with open("example_pipline.dsl", "r") as file:
        dsl_definition = file.read()

    logic_registry = {
        "summator_logic": summator_logic,
        "multiplier_logic": multiplier_logic,
    }

    interpreter = DSLPipelineInterpreter(dsl_definition, logic_registry)
    pipeline, initial_data = interpreter.interpret()

    pipeline.run(initial_data)

    product_channel = pipeline.channels["product"]
    result = product_channel.receive()
    print(f"Результат: {result}")
