FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y  --no-install-recommends \
        netcat postgresql curl git ssh  software-properties-common \
        make build-essential ca-certificates libpq-dev && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y \
        python python-dev python-pip \
        python3.6 python3.6-dev python3-pip python3.6-venv \
        python3.7 python3.7-dev python3.7-venv \
        python3.8 python3.8-dev python3.8-venv && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN useradd -mU dbt_test_user
RUN mkdir /usr/app && chown dbt_test_user /usr/app
RUN mkdir /home/tox && chown dbt_test_user /home/tox

WORKDIR /usr/app
VOLUME /usr/app

RUN pip3 install tox wheel

RUN python2.7 -m pip install virtualenv wheel && \
    python2.7 -m virtualenv /home/tox/venv2.7 && \
    /home/tox/venv2.7/bin/python -m pip install -U pip tox

RUN python3.6 -m pip install -U pip wheel && \
    python3.6 -m venv /home/tox/venv3.6 && \
    /home/tox/venv3.6/bin/python -m pip install -U pip tox

RUN python3.7 -m pip install -U pip wheel && \
    python3.7 -m venv /home/tox/venv3.7 && \
    /home/tox/venv3.7/bin/python -m pip install -U pip tox

RUN python3.8 -m pip install -U pip wheel && \
    python3.8 -m venv /home/tox/venv3.8 && \
    /home/tox/venv3.8/bin/python -m pip install -U pip tox

USER dbt_test_user

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
