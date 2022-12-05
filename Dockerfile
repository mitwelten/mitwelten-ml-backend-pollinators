FROM ultralytics/yolov5:latest

RUN apt-get update
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Zurich
RUN apt-get install -y wget tzdata

WORKDIR /root

# download model weights and place it into correct folder
RUN python -m pip install gdown
RUN python ./src/scripts/download_models.py

# copy content from local repo
COPY . .

# install relevant packages
RUN python -m pip install -r requirements.txt

# Expose ports
# prefect, jlab
EXPOSE 4200 8888

ENTRYPOINT [ "bash" ]
