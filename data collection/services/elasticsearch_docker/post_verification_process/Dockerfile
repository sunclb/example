FROM python:3.6.8-slim-jessie
COPY . /app
WORKDIR /app
ENV function_file=post_batch_process.py 
#RUN apt-get update && apt-get install -y software-properties-common
#RUN add-apt-repository ppa:deadsnakes/ppa
#RUN apt-get update && apt-get install -y build-essential python3.6 python3.6-dev python3-pip python3.6-venv
#RUN curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.6
RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install --trusted-host pypi.python.org -r requirements.txt
CMD ["sh","-c","python $function_file"]

