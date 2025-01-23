pipeline:
  name: ExamplePipeline
  nodes:
    - name: Summator
      inputs: [input1, input2]
      outputs: [sum]
      logic: summator_logic

    - name: Multiplier
      inputs: [sum]
      outputs: [product]
      logic: multiplier_logic

  channels:
    - name: input1
      type: int
    - name: input2
      type: int
    - name: sum
      type: int
    - name: product
      type: int

  initial_data:
    input1: 2
    input2: 5