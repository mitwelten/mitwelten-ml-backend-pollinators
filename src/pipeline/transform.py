import os
import shutil
import yaml

import pandas as pd
from PIL import Image
import PIL
from tqdm import tqdm

if __name__ =='__main__':
    from Pollinatordetection import YoloModel
else:
    from .Pollinatordetection import YoloModel


def dummy_transform(remove_dir):
    shutil.rmtree(path=remove_dir, ignore_errors=True)

def init_models(cfg: dict):
    # Flower Model configuration
    model_flower_config = cfg.get("flower")
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

    model_pollinator_config = cfg.get("pollinator")
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


def model_predict(data: pd.DataFrame, cfg: dict):

    flower_model, pollinator_model = init_models(cfg=cfg)

    # Initiate
    flower_predictions, pollinator_predictions = [], []

    with tqdm(total=len(data)) as pbar:

        for idx in data.index:
            filename = data.loc[idx, 'object_name']
            pbar.set_description(f'Processing File {filename}')

            flower_model.reset_inference_times()
            pollinator_model.reset_inference_times()
            # predict flower
            try:
                img = Image.open(filename)
                original_width, original_height = img.size
            except Exception as e:
                print(e(f'Not able to load image {filename}'))
                pbar.update(1)
                continue
            try:
                flower_model.predict(img)
            except Exception as e:
                print(e(f'Could not do inference for {filename}'))
                pbar.update(1)
                continue
            
            flower_crops = flower_model.get_crops()
            flower_boxes = flower_model.get_boxes()
            flower_classes = flower_model.get_classes()
            flower_scores = flower_model.get_scores()
            flower_names = flower_model.get_names()

            for flower_index in range(len(flower_crops)):
                width, height = (
                    flower_crops[flower_index].shape[1],
                    flower_crops[flower_index].shape[0],
                )

                # write flower predictions dataframe
                flower_predictions.append(
                    {
                        'object_name': filename,
                        'flower_box_id': flower_index,
                        'flower_box': flower_boxes[flower_index],
                        'flower_class': flower_classes[flower_index],
                        'flower_score': flower_scores[flower_index],
                        'flower_name': flower_names[flower_index],
                        'width': width,
                        'height': height
                    }
                )
                # predict pollinator
                pollinator_model.predict(flower_crops[flower_index])

                if len(pollinator_model.get_boxes()) > 0:
                    for i in range(len(pollinator_model.get_boxes())):
                        pollinator_predictions.append(
                            {
                                'object_name': filename,
                                'flower_box_id': flower_index,
                                'pollinator_boxes' : pollinator_model.get_boxes()[i],
                                'pollinator_classes' : pollinator_model.get_classes()[i],
                                'pollinator_scores' : pollinator_model.get_scores()[i],
                                'pollinator_names' : pollinator_model.get_names()[i],
                            }
                        )

            pbar.update(1)

    return flower_predictions, pollinator_predictions

def process_flower_predictions(flower_predictions: dict, result_ids: pd.DataFrame, model_config: float) -> pd.DataFrame:
    """
    Post-processing of flower predictions outputet as json.

    Parameters
    ----------
    flower_predictions : dict (json)
        Predictions from Yolo for a certain flower.

    result_ids : pd.DataFrame
        Queried data with existing result_ids

    model_config: dict
        Relevant margin which was used to crop the flower for further
        prediction of pollinators (located in model_config.json)

    Returns
    -------
    pd.DataFrame
        processed dataframe ready for db insertion
    """
    box_margin = model_config['flower']['margin']
    # Preprocess results from model_predict
    flower_predictions = pd.DataFrame.from_records(flower_predictions)

    # Join with DB data
    flower_predictions = pd.merge(
        left=result_ids,
        right=flower_predictions,
        left_on='object_name',
        right_on='object_name',
        how='right'
    )
    # extract bbox data
    flower_predictions['x0'] = flower_predictions['flower_box'].apply(lambda x: x[0]).astype(int)
    flower_predictions['y0'] = flower_predictions['flower_box'].apply(lambda x: x[1]).astype(int)
    flower_predictions['x1'] = flower_predictions['flower_box'].apply(lambda x: x[2]).astype(int)
    flower_predictions['y1'] = flower_predictions['flower_box'].apply(lambda x: x[3]).astype(int)

    flower_predictions = flower_predictions.drop('flower_box', axis=1)

    return flower_predictions

def process_pollinator_predictions(pollinator_predictions: dict, flower_predictions: pd.DataFrame, model_config: dict = None) -> pd.DataFrame:
    """
    Processes pollinator predictions. Adds relevant columsn for DB ingestion and transforms the bbox outputs given the
    the bbox outputs from flower_predictions.

    Parameters
    ----------
    pollinator_predictions : dict
        Modell output pollinator predictions

    flower_predictions : pd.DataFrame
        Pre-processed flower predictions model output

    model_config: dict
        Relevant margin which was used to crop the flower for further
        prediction of pollinators (located in model_config.json)

    Returns
    -------
    pd.DataFrame
        processed pollinator predictions ready for DB ingestion.
    """
    # get flower margin for the correct offset of the pollinator box
    # add box margin relative to width/heigth, which ensures the correct bbox for all
    # pollinators as they are relative to the flower bbox + margin
    box_margin = model_config['flower']['margin']
    flower_predictions['x0'] = flower_predictions['x0'] - box_margin
    flower_predictions['y0'] = flower_predictions['y0'] - box_margin
    flower_predictions['x1'] = flower_predictions['x1'] + box_margin
    flower_predictions['y1'] = flower_predictions['y1'] + box_margin

    pollinator_predictions = pd.DataFrame.from_records(pollinator_predictions)

    # Join with DB data
    # Merge with flowers to get size of BB
    pollinator_predictions = pd.merge(
        left=pollinator_predictions,
        right=flower_predictions[['object_name', 'flower_box_id', 'result_id', 'flower_id', 'x0', 'y0', 'x1', 'y1']],
        left_on=['object_name', 'flower_box_id'], right_on=['object_name', 'flower_box_id'],
        how='inner'
    )
    # rename columns
    pollinator_predictions = pollinator_predictions.rename(
        columns={
            'x0': 'f_x0', 'x1': 'f_x1',
            'y0': 'f_y0', 'y1': 'f_y1'
        }
    )
    # Calculate BBoxes given previous flower perdiction
    pollinator_predictions['p_x0'] = pollinator_predictions['pollinator_boxes'].apply(lambda x: x[0]).astype(int)
    pollinator_predictions['p_y0'] = pollinator_predictions['pollinator_boxes'].apply(lambda x: x[1]).astype(int)
    pollinator_predictions['p_x1'] = pollinator_predictions['pollinator_boxes'].apply(lambda x: x[2]).astype(int)
    pollinator_predictions['p_y1'] = pollinator_predictions['pollinator_boxes'].apply(lambda x: x[3]).astype(int)

    # new_xmin = flower_xmin + polli_xmin / new_ymin = flower_ymin + polli_ymin
    pollinator_predictions['x0'] = pollinator_predictions['f_x0'] + pollinator_predictions['p_x0']
    pollinator_predictions['y0'] = pollinator_predictions['f_y0'] + pollinator_predictions['p_y0']
    # new_xmax = flower_xmin + polli_xmax / new_ymax = flower_xmin + polli_ymax
    pollinator_predictions['x1'] = pollinator_predictions['f_x0'] + pollinator_predictions['p_x1']
    pollinator_predictions['y1'] = pollinator_predictions['f_y0'] + pollinator_predictions['p_y1']

    pollinator_predictions = pollinator_predictions.drop(
        [
            'p_x0', 'p_x1', 'p_y0', 'p_y1', 'f_x0', 'f_x1', 'f_y0',
            'f_y1', 'pollinator_boxes', 'pollinator_classes'
        ],
        axis=1
    )

    return pollinator_predictions
