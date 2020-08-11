# See all tag variants at https://hub.docker.com/r/tensorflow/tensorflow/tags/
FROM tensorflow/tensorflow:1.14.0-gpu-py3-jupyter
RUN pip install elasticsearch
RUN pip install multiprocess
RUN pip install tensor2tensor[tensorflow_gpu]
RUN pip install PyYAML

# -m option creates a fake writable home folder for Jupyter.
RUN groupadd -g 1004 huiyuan && \
    useradd -m -r -u 1003 -g huiyuan huiyuan
USER huiyuan

VOLUME ["/Code"]
#COPY speech_recognition.py /usr/local/lib/python3.6/dist-packages/tensor2tensor/data_generators/speech_recognition.py
WORKDIR /Code
#RUN pip install -r requirements.txt


CMD ["jupyter", "notebook", "--no-browser", "--ip=0.0.0.0", \
     "/Code"]
