
![MIT License](https://img.shields.io/badge/Organization-Mitwelten-green)


# Mitwelten: pollinator-ml-backend

This projects implements a backend infrastructure which applies an ETL pipeline for the [PollinatorDetection Model](https://github.com/WullT/Pollinatordetection). The pipeline processes data from an s3 bucket and inserts it to a SQL database.

## Installation

Clone repo locally
```bash
git clone git@github.com:mitwelten/pollinator-ml-backend.git
cd pollinator-ml-backend
```

**Note**:
The follow-up instructions are limited to Linux OS. It is not guranteed to work with Windows (particularly nvidia-docker). In my case on Windows OS I installed the Linux subsystem for Windows (Ubuntu 22.04) and it worked fine.

### Docker setup with GPU (recommended)
Make sure nvidia-docker is installed to make use of GPU during yolov5 inference. 

Requirements (according to [yolov5 instructions](https://github.com/ultralytics/yolov5/wiki/Docker-Quickstart)):

- Nvidia Driver >= 455.23 https://www.nvidia.com/Download/index.aspx
- Nvidia-Docker https://github.com/NVIDIA/nvidia-docker
- Docker Engine - CE >= 19.03 https://docs.docker.com/install/

Build from Dockerfile
```bash
docker build . --file Dockerfile --tag pollinator-ml-backend-image
```

Start container
```bash
docker run -it -v $PWD:/root/ --name pollinator-ml-backend --gpus all --net=host pollinator-ml-backend-image
```

Inside Container CLI check CUDA version
```bash
nvidia-smi

```
The output should look like this:
```bash
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 520.56.06    Driver Version: 522.30       CUDA Version: 11.8     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|                               |                      |               MIG M. |
|===============================+======================+======================|
|   0  NVIDIA GeForce ...  On   | 00000000:09:00.0  On |                  N/A |
|  0%   53C    P8    30W / 200W |    980MiB /  8192MiB |      0%      Default |
|                               |                      |                  N/A |
+-------------------------------+----------------------+----------------------+

+-----------------------------------------------------------------------------+
| Processes:                                                                  |
|  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
|        ID   ID                                                   Usage      |
|=============================================================================|
|  No running processes found                                                 |
+-----------------------------------------------------------------------------+
```

### Docker setup CPU only

Build from Dockerfile
```bash
docker build . --file Dockerfile --tag pollinator-ml-backend-image
```

Start container
```bash
docker run -it -v $PWD:/root/ --name pollinator-ml-backend pollinator-ml-backend-image
```

### Manual setup with python pip

Setup virtual environment 

```bash
python -m venv env
```

Activate virtual environment
```bash
# Linux
source env/bin/activate

# Windows
env\Scripts\activate
```

Install pytorch according to official instructions: https://pytorch.org/get-started/locally/

Install project specific packages:
```bash
python pip install -r requirements.txt
```
## Deployment (to be added)

## Run Locally (to be added)


## Authors

- [@simsta1](https://www.github.com/https://github.com/simsta1)
- [@WullT](https://github.com/WullT)

## Resources

https://github.com/WullT/Pollinatordetection

https://github.com/ultralytics/yolov5
