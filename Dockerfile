FROM python:3.5

RUN apt-get update

RUN apt-get install -y python-pip netcat
RUN apt-get install -y python-dev python3-dev

RUN pip install pip --upgrade
RUN pip install virtualenv
RUN pip install virtualenvwrapper
RUN pip install tox

WORKDIR /usr/src/app
RUN cd /usr/src/app
