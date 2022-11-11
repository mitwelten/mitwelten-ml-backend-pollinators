FROM ultralytics/yolov5:latest

RUN apt-get update 

WORKDIR /root

# download model weights and place it into correct folder
RUN python -m pip install gdown
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1aJqHIP2s0TH8cDBIRouM6rjC8ZF4VWjg/view?usp=share_link --fuzzy
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1PAubLPaEAHALgmo2KkEzuTOakv_AmqUu/view?usp=share_link --fuzzy
RUN gdown -O ./src/etl/transform/PollinatorDetection/models/ https://drive.google.com/file/d/1xHiwc5PmYGwj6AImyy2zljev_DZtuLcJ/view?usp=share_link --fuzzy

# copy content from local repo
COPY . .

# install relevant packages
RUN python -m pip install -r requirements.txt

# Expose ports
# prefect, jlab, postgres, minio
EXPOSE 4200 8888 5432 9000

ENTRYPOINT [ "bash" ]
