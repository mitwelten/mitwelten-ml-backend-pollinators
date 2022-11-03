import os
import shutil
import yaml

import pandas as pd

from prefect import task

from PIL import Image

if __name__ =='__main__':
    from Pollinatordetection import YoloModel
else:    
    from .Pollinatordetection import YoloModel
    
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

    # Initiate Dataframes
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
                flower_model.predict(img)
            except Exception as e:
                print(Exception(f'Could not infer {filename}'))
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
                        'flower_obj_id': flower_index, 
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
                                'flower_id': flower_index,
                                'pollinator_boxes' : pollinator_model.get_boxes()[i],
                                'pollinator_classes' : pollinator_model.get_classes()[i],
                                'pollinator_scores' : pollinator_model.get_scores()[i],
                                'pollinator_names' : pollinator_model.get_names()[i],
                            }
                        )
                        
            pbar.update(1)

    return flower_predictions, pollinator_predictions            


if __name__ == '__main__':
    
    with open('model_config.yaml', 'rb') as yaml_file:
        model_config = yaml.load(yaml_file, Loader=yaml.FullLoader)

    df = pd.read_csv('synthetic_table.csv')
    available_files = os.listdir('/home/simon/work/repo/pollinator-ml-backend/0344-6782/2021-06-21/08')
    available_files = [os.path.join('0344-6782/2021-06-21/08', f) for f in available_files]
    res = df.loc[df['object_name'].isin(available_files)]

    flower, pollinators = model_predict(data=res.iloc[:20],  cfg=model_config)
    print(10*'--')
    print(pollinators)