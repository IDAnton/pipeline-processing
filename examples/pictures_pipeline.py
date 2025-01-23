import cv2
import numpy as np
from typing import Dict, Any

from pipeline.pipeline import TypedChannel, Node, Pipeline


def load_image(path: str) -> np.ndarray:
    return cv2.imread(path)

def save_image(path: str, image: np.ndarray):
    cv2.imwrite(path, image)

def denoise_filter(data: Dict[str, Any]) -> Dict[str, Any]:
    img = data["image"]
    denoised = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
    return {"denoised": denoised}

def convert_to_jpg(data: Dict[str, Any]) -> Dict[str, Any]:
    img = data["image"]
    _, encoded = cv2.imencode(".jpg", img)
    decoded = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
    return {"jpg_image": decoded}

def image_processing_pipeline():
    pipeline = Pipeline(name="ImageProcessingPipeline")

    raw_channel = pipeline.add_channel("image", np.ndarray)
    denoised_channel = pipeline.add_channel("denoised", np.ndarray)
    jpg_channel = pipeline.add_channel("jpg_image", np.ndarray)

    denoiser = Node(
        name="Denoiser",
        inputs=[raw_channel],
        outputs=[denoised_channel],
        process_function=denoise_filter
    )

    jpg_converter = Node(
        name="JPGConverter",
        inputs=[denoised_channel],
        outputs=[jpg_channel],
        process_function=convert_to_jpg
    )

    pipeline.add_node(denoiser)
    pipeline.add_node(jpg_converter)

    raw_image = load_image("image.png")
    pipeline.run(initial_data={"image": raw_image})

    res = pipeline.channels["jpg_image"].receive()
    save_image("result.jpg", res)


if __name__ == "__main__":
    image_processing_pipeline()
