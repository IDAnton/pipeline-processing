import unittest
from typing import Dict, Any

from pipeline import TypedChannel, Node, Pipeline


class TestTypedChannel(unittest.TestCase):
    def test_send_and_receive_valid_data(self):
        channel = TypedChannel("test_channel", int)
        channel.send(42)
        self.assertEqual(channel.receive(), 42)

    def test_send_invalid_data_raises_error(self):
        channel = TypedChannel("test_channel", int)
        with self.assertRaises(TypeError):
            channel.send("not_an_int")


class TestNode(unittest.TestCase):
    def test_node_process(self):
        # Создаём входные и выходные каналы
        input1 = TypedChannel("input1", int)
        input2 = TypedChannel("input2", int)
        sum_channel = TypedChannel("sum", int)

        def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"sum": data["input1"] + data["input2"]}

        summator = Node(
            name="Summator",
            inputs=[input1, input2],
            outputs=[sum_channel],
            process_function=summator_logic
        )

        input1.send(10)
        input2.send(10)

        summator.process()

        self.assertEqual(sum_channel.receive(), 20)


class TestPipeline(unittest.TestCase):
    def test_pipeline_execution(self):
        pipeline = Pipeline(name="TestPipeline")

        input1 = pipeline.add_channel("input1", int)
        input2 = pipeline.add_channel("input2", int)
        sum_channel = pipeline.add_channel("sum", int)
        product_channel = pipeline.add_channel("product", int)

        def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"sum": data["input1"] + data["input2"]}

        def multiplier_logic(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"product": data["sum"] * 2}

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

        initial_data = {"input1": 10, "input2": 5}
        pipeline.run(initial_data)

        self.assertEqual(product_channel.receive(), 30)

    def test_pipeline_with_invalid_data(self):
        pipeline = Pipeline(name="TestPipeline")

        input1 = pipeline.add_channel("input1", int)
        input2 = pipeline.add_channel("input2", int)
        sum_channel = pipeline.add_channel("sum", int)

        def summator_logic(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"sum": data["input1"] + data["input2"]}

        summator = Node(
            name="Summator",
            inputs=[input1, input2],
            outputs=[sum_channel],
            process_function=summator_logic
        )

        pipeline.add_node(summator)

        initial_data = {"input1": 10, "input2": "not_an_int"}
        with self.assertRaises(TypeError):
            pipeline.run(initial_data)


if __name__ == "__main__":
    unittest.main()
