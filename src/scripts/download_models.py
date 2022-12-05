import gdown

if __name__ == '__main__':

    gdown.download(
        url='https://drive.google.com/file/d/1aJqHIP2s0TH8cDBIRouM6rjC8ZF4VWjg/view?usp=share_link', 
        output='./src/pipeline/Pollinatordetection/models/flower_n.pt',
        fuzzy=True
    )
    gdown.download(
        url='https://drive.google.com/file/d/1PAubLPaEAHALgmo2KkEzuTOakv_AmqUu/view?usp=share_link', 
        output='./src/pipeline/Pollinatordetection/models/flowers_ds_v5_640_yolov5n_box_hyps_v0.pt',
        fuzzy=True
    )
    gdown.download(
        url='https://drive.google.com/file/d/1xHiwc5PmYGwj6AImyy2zljev_DZtuLcJ/view?usp=share_link', 
        output='./src/pipeline/Pollinatordetection/models/pollinators_ds_v6_480_yolov5s_hyps_v0.pt',
        fuzzy=True
    )