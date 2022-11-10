FROM ultralytics/yolov5:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    wget \
    ffmpeg \
    libsm6 \
    libxext6

WORKDIR /root

# download model weights and place it into correct folder
RUN python -m pip install gdown
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1aJqHIP2s0TH8cDBIRouM6rjC8ZF4VWjg/view?usp=share_link
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1PAubLPaEAHALgmo2KkEzuTOakv_AmqUu/view?usp=share_link
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1xHiwc5PmYGwj6AImyy2zljev_DZtuLcJ/view?usp=share_link

# install relevant packages
RUN python -m pip install -r requirements.txt

# copy the content of the local directory to the working directory
COPY . .

ENTRYPOINT [ "bash" ]