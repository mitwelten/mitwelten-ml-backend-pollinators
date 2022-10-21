import os
import shutil
import time
import yaml

import pandas as pd

from prefect import task

import sys
sys.path.append('./Pollinatordetection')

from PIL import Image
from .Pollinatordetection.yolomodelhelper import YoloModel
from .Pollinatordetection.messagehelper import Flower, Pollinator
from .Pollinatordetection.inputs import DirectoryInput
from tqdm import tqdm



@task
def dummy_transform(remove_dir):
    shutil.rmtree(path=remove_dir, ignore_errors=True)

def init_models(cfg: dict):
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

    return flower_model, pollinator_model


@task
def model_predict(data: pd.DataFrame, cfg: dict): 
    flower_model, pollinator_model = init_models(cfg=cfg) 
    # Output Configuration
    output_config = cfg.get("output")
    # Output Configuration (File)
    BASE_DIR = output_config.get('base_dir')
    SAVE_CROPS = True
    if output_config.get("file") is not None:
        output_config_file = output_config.get("file")
        if output_config_file.get("store_file", False):
            STORE_FILE = True
            BASE_DIR = output_config_file.get("base_dir", "output")
            SAVE_CROPS = output_config_file.get("save_crops", True)

    # create dataframe to save predictions
    predictions = pd.DataFrame(
        columns=['object_name', 'class_name', 'box', 'score']
    )

    # is there a way to process this in batches??
    for idx in data.index:
        filename = data.loc[idx, 'object_name']
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

        flower_predictions = {filename: []}

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
            pollinator_model.predict(flower_crops[flower_index])
            # predict pollinator
            if len(pollinator_model.get_boxes()) > 0:    
                pollinator_data = {
                    'pollinator_boxes' : pollinator_model.get_boxes(),
                    'pollinator_classes' : pollinator_model.get_classes(),
                    'pollinator_scores' : pollinator_model.get_scores(),
                    'pollinator_names' : pollinator_model.get_names()
                }
                print(pollinator_data)
                

                flower_predictions[filename].append(pollinator_data)
            
        print(flower_predictions)
        #tmp = pd.DataFrame.from_records(pollinator_data)
        #predictions = pd.concat([predictions, tmp], axis=0)
            

    return predictions

if __name__ == '__main__':

    with open('./Pollinatordetection/config.yaml', 'rb') as yaml_file:
        model_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    model_predict(input_filepaths=os.listdir('0344-6782'), cfg=model_config)