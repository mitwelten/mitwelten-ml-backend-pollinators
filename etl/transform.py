import os
import shutil
import time
import yaml

from prefect import task

import sys
sys.path.append('./Pollinatordetection')

from PIL import Image
from yolomodelhelper import YoloModel
from messagehelper import Flower, Pollinator
from inputs import DirectoryInput
from tqdm import tqdm



@task
def dummy_transform(remove_dir):
    shutil.rmtree(path=remove_dir, ignore_errors=True)


#@task
def model_predict(input_filepaths, cfg):
    # Flower Model configuration
    model_flower_config = cfg.get("models").get("flower")
    MODEL_FLOWER_WEIGHTS = model_flower_config.get("weights_path")
    MODEL_FLOWER_CLASS_NAMES = model_flower_config.get("class_names")
    MODEL_FLOWER_CONFIDENCE_THRESHOLD = model_flower_config.get("confidence_threshold")
    MODEL_FLOWER_IOU_THRESHOLD = model_flower_config.get("iou_threshold")
    MODEL_FLOWER_MARGIN = model_flower_config.get("margin")
    MODEL_FLOWER_MULTI_LABEL = model_flower_config.get("multi_label")
    MODEL_FLOWER_MULTI_LABEL_IOU_THRESHOLD = model_flower_config.get(
        "multi_label_iou_threshold"
    )
    MODEL_FLOWER_MAX_DETECTIONS = model_flower_config.get("max_detections")
    MODEL_FLOWER_AUGMENT = model_flower_config.get("augment", False)
    MODEL_FLOWER_IMG_SIZE = model_flower_config.get("image_size")

    model_pollinator_config = cfg.get("models").get("pollinator")
    MODEL_POLLINATOR_WEIGHTS = model_pollinator_config.get("weights_path")
    MODEL_POLLINATOR_CLASS_NAMES = model_pollinator_config.get("class_names")
    MODEL_POLLINATOR_IMG_SIZE = model_pollinator_config.get("image_size")
    MODEL_POLLINATOR_CONFIDENCE_THRESHOLD = model_pollinator_config.get(
        "confidence_threshold"
    )
    MODEL_POLLINATOR_IOU_THRESHOLD = model_pollinator_config.get("iou_threshold")
    MODEL_POLLINATOR_MARGIN = model_pollinator_config.get("margin")
    MODEL_POLLINATOR_AUGMENT = model_pollinator_config.get("augment", False)
    MODEL_POLLINATOR_MAX_DETECTIONS = model_pollinator_config.get("max_detections")
    MODEL_POLLINATOR_MULTI_LABEL = model_pollinator_config.get("multi_label")
    MODEL_POLLINATOR_MULTI_LABEL_IOU_THRESHOLD = model_pollinator_config.get(
        "multi_label_iou_threshold"
    )


    # Input Configuration
    dir_input = None

    # Directory Input Configuration
    INPUT_DIRECTORY_BASE_DIR = cfg.get('input').get('directory').get('base_dir')
    INPUT_DIRECTORY_EXTENSION = '.jpg'
    dir_input = DirectoryInput(INPUT_DIRECTORY_BASE_DIR, INPUT_DIRECTORY_EXTENSION)
    dir_input.scan()

    # Output Configuration
    output_config = cfg.get("output")
    IGNORE_EMPTY_RESULTS = output_config.get("ignore_empty_results", False)
    # Output Configuration (File)
    STORE_FILE = False
    BASE_DIR = "output"
    SAVE_CROPS = True
    if output_config.get("file") is not None:
        output_config_file = output_config.get("file")
        if output_config_file.get("store_file", False):
            STORE_FILE = True
            BASE_DIR = output_config_file.get("base_dir", "output")
            SAVE_CROPS = output_config_file.get("save_crops", True)


    # Init Flower Model
    flower_model = YoloModel(
        MODEL_FLOWER_WEIGHTS,
        image_size=MODEL_FLOWER_IMG_SIZE,
        confidence_threshold=MODEL_FLOWER_CONFIDENCE_THRESHOLD,
        iou_threshold=MODEL_FLOWER_IOU_THRESHOLD,
        margin=MODEL_FLOWER_MARGIN,
        class_names=MODEL_FLOWER_CLASS_NAMES,
        multi_label=MODEL_FLOWER_MULTI_LABEL,
        multi_label_iou_threshold=MODEL_FLOWER_MULTI_LABEL_IOU_THRESHOLD,
        augment=MODEL_FLOWER_AUGMENT,
        max_det=MODEL_FLOWER_MAX_DETECTIONS,
    )

    # Init Pollinator Model
    pollinator_model = YoloModel(
        MODEL_POLLINATOR_WEIGHTS,
        image_size=MODEL_POLLINATOR_IMG_SIZE,
        confidence_threshold=MODEL_POLLINATOR_CONFIDENCE_THRESHOLD,
        iou_threshold=MODEL_POLLINATOR_IOU_THRESHOLD,
        margin=MODEL_POLLINATOR_MARGIN,
        class_names=MODEL_POLLINATOR_CLASS_NAMES,
        multi_label=MODEL_POLLINATOR_MULTI_LABEL,
        multi_label_iou_threshold=MODEL_POLLINATOR_MULTI_LABEL_IOU_THRESHOLD,
        augment=MODEL_POLLINATOR_AUGMENT,
        max_det=MODEL_POLLINATOR_MAX_DETECTIONS,
    )
    loop_through = True
    while loop_through:
        filename = dir_input.get_next()
        if filename is not None:
            flower_model.reset_inference_times()
            pollinator_model.reset_inference_times()
            pollinator_index = 0
            # predict flower
            try:
                img = Image.open(filename)
                original_width, original_height = img.size
                flower_model.predict(img)
            except Exception as e:
                continue
            flower_crops = flower_model.get_crops()
            flower_boxes = flower_model.get_boxes()
            flower_classes = flower_model.get_classes()
            flower_scores = flower_model.get_scores()
            flower_names = flower_model.get_names()
            for flower_index in tqdm(range(len(flower_crops))):
                # add flower to message
                # TODO: add flower to message
                width, height = (
                    flower_crops[flower_index].shape[1],
                    flower_crops[flower_index].shape[0],
                )
                flower_obj = Flower(
                    index=flower_index,
                    class_name=flower_names[flower_index],
                    score=flower_scores[flower_index],
                    width=width,
                    height=height,
                )
                # predict pollinator
                pollinator_model.predict(flower_crops[flower_index])
                pollinator_boxes = pollinator_model.get_boxes()
                pollinator_crops = pollinator_model.get_crops()
                pollinator_classes = pollinator_model.get_classes()
                pollinator_scores = pollinator_model.get_scores()
                pollinator_names = pollinator_model.get_names()
                pollinator_indexes = pollinator_model.get_indexes()
                for detected_pollinator in range(len(pollinator_crops)):
                    idx = pollinator_index + pollinator_indexes[detected_pollinator]
                    crop_image = Image.fromarray(pollinator_crops[detected_pollinator])
                    width_polli, height_polli = crop_image.size
                    # add pollinator to message
                    pollinator_obj = Pollinator(
                        index=idx,
                        flower_index=flower_index,
                        class_name=pollinator_names[detected_pollinator],
                        score=pollinator_scores[detected_pollinator],
                        width=width_polli,
                        height=height_polli,
                        crop=crop_image,
                    )
                if len(pollinator_indexes) > 0:
                    pollinator_index += max(pollinator_indexes) + 1

        else:
            time.sleep(5)

if __name__ == '__main__':

    with open('./Pollinatordetection/config.yaml', 'rb') as yaml_file:
        model_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    
    model_predict(input_filepaths=os.listdir('0344-6782'), cfg=model_config)

