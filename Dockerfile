FROM ultralytics/yolov5:latest

RUN apt-get update
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
RUN apt-get install -y wget tzdata

WORKDIR /root

# download model weights and place it into correct folder
RUN python -m pip install gdown
RUN gdown -O ./src/pipeline/Pollinatordetection/models/flower_n.pt https://drive.google.com/file/d/1aJqHIP2s0TH8cDBIRouM6rjC8ZF4VWjg/view?usp=share_link --fuzzy
RUN gdown -O ./src/pipeline/Pollinatordetection/models/flowers_ds_v5_640_yolov5n_box_hyps_v0.pt https://drive.google.com/file/d/1PAubLPaEAHALgmo2KkEzuTOakv_AmqUu/view?usp=share_link --fuzzy
RUN gdown -O ./src/pipeline/Pollinatordetection/models/pollinators_ds_v6_480_yolov5s_hyps_v0.pt https://drive.google.com/file/d/1xHiwc5PmYGwj6AImyy2zljev_DZtuLcJ/view?usp=share_link --fuzzy

# copy content from local repo
COPY . .

# install relevant packages
RUN python -m pip install -r requirements.txt

# Expose ports
# prefect, jlab
EXPOSE 4200 8888

ENTRYPOINT [ "bash" ]
