FROM python:2.7

RUN apt-get update
RUN apt-get install -y python-pip netcat
RUN pip install pip --upgrade
RUN pip install virtualenv
RUN pip install virtualenvwrapper

COPY . /usr/src/app

WORKDIR /usr/src/app
RUN cd /usr/src/app
RUN ./test/setup.sh
