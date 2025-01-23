import cv2
from ultralytics import YOLO

def patch_bbox_to_global(bbox, patch_shape, image_shape, patch_x, patch_y):
    print(image_shape)
    print([patch_x, patch_y])
    print(bbox)
    x = (bbox[0] * patch_shape[1] + patch_x) / image_shape[1]
    y = (bbox[1] * patch_shape[0] + patch_y) / image_shape[0]
    w = (bbox[2] * patch_shape[1]) / image_shape[1]
    h = (bbox[3] * patch_shape[0]) / image_shape[0]
    print([x, y, w, h])
    return [x, y, w, h]

def calc_iou(bbox_1, bbox_2):
    inter_w = 0
    inter_h = 0
    if (bbox_1[0] < bbox_2[0]):
        inter_w = max(min(bbox_1[0] + bbox_1[2] - bbox_2[0], bbox_2[2]), 0)
    else:
        inter_w = max(min(bbox_2[0] + bbox_2[2] - bbox_1[0], bbox_1[2]), 0)
    if (bbox_1[1] < bbox_2[1]):
        inter_h = max(min(bbox_1[1] + bbox_1[3] - bbox_2[1], bbox_2[3]), 0)
    else:
        inter_h = max(min(bbox_2[1] + bbox_2[3] - bbox_1[1], bbox_1[3]), 0)
    inter_s = inter_w * inter_h
    iou = inter_s / (bbox_1[2] * bbox_1[3] + bbox_2[2] * bbox_2[3] - inter_s)
    return iou
    
def detect(image, model='yolo11n.pt'):
    model = YOLO(model)
    result = model(image)
    boxes = result[0].boxes
    result = {}
    classes = [int(i) for i in list(boxes.cls.cpu().numpy())]
    xywhn = [list(i) for i in list(boxes.xywhn.cpu().numpy())]
    for i in range(len(classes)):
        c = classes[i]
        bbox = xywhn[i]
        if (c in result.keys()):
            result[c].append(bbox)
        else:
            result.update({c : [bbox]})
    return {'image' : image, 'result' : result}

def patched_detect(image, 
                   patch_x = 640, 
                   patch_y = 640, 
                   overlap_x = 0.1,
                   overlap_y = 0.1,
                   model='yolo11n.pt',
                   iou_treshold = 0.2):
    model = YOLO(model)
    result = {}
    confs = {}
    
    image_x = image.shape[1]
    image_y = image.shape[0]
    overlap_x_p = int(patch_x * overlap_x)
    overlap_y_p = int(patch_y * overlap_y)
    n_patches_x = 1
    n_patches_y = 1
    if (image_x > patch_x):
        n_patches_x += round((image_x - patch_x) / (patch_x - overlap_x_p))
    if (image_y > patch_y):
        n_patches_y += round((image_y - patch_y) / (patch_y - overlap_y_p))
    
    resize_x = patch_x + (n_patches_x - 1) * (patch_x - overlap_x_p)
    resize_y = patch_y + (n_patches_y - 1) * (patch_y - overlap_y_p)
    resized_image = cv2.resize(image, [resize_x, resize_y])
    
    for n_x in range(n_patches_x):
        for n_y in range(n_patches_y):
            patch = resized_image[n_y * (patch_y - overlap_y_p):n_y * (patch_y - overlap_y_p) + patch_y, n_x * (patch_x - overlap_x_p):n_x * (patch_x - overlap_x_p) + patch_x]
            patch_result = model(patch)
            patch_boxes = patch_result[0].boxes
            patch_classes = [int(i) for i in list(patch_boxes.cls.cpu().numpy())]
            patch_xywhn = [list(i) for i in list(patch_boxes.xywhn.cpu().numpy())]
            patch_conf = [i for i in list(patch_boxes.conf.cpu().numpy())]
            for i in range(len(patch_classes)):
                c = patch_classes[i]
                bbox = patch_bbox_to_global(patch_xywhn[i], patch.shape, resized_image.shape, n_x * (patch_x - overlap_x_p), n_y * (patch_y - overlap_y_p))
                if (c in result.keys()):
                    bbox_pass = 1
                    for j in range(len(result[c])):
                        bbox_2 = result[c][j]
                        if (calc_iou(bbox, bbox_2) > iou_treshold):
                            if (patch_conf[i] > confs[c][j]):
                                result[c][j] = bbox
                                confs[c][j] = patch_conf[i]
                                bbox_pass = 0
                                break
                            else:
                                bbox_pass = 0
                                break
                    if (bbox_pass == 1):
                        result[c].append(bbox)
                        confs[c].append(patch_conf[i])
                else:
                    result.update({c : [bbox]})
                    confs.update({c : [patch_conf[i]]})
    return {'image' : image, 'result' : result}