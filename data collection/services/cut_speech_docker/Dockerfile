FROM ubuntu:16.04
COPY . /app
WORKDIR /app
ENV function_file=None input_folder=None output_folder=./cut_output

RUN apt-get update && apt-get install -y software-properties-common
RUN add-apt-repository ppa:jonathonf/ffmpeg-4
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y ffmpeg
RUN apt-get update && apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
#RUN curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.6
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install --trusted-host pypi.python.org -r requirement.txt
CMD ["sh","-c","python3.6 $function_file \
    --input_folder=$input_folder \
    --output_folder=$output_folder"]

