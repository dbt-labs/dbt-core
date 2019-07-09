FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y  --no-install-recommends \
        netcat postgresql make build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
        xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev git ca-certificates \
        curl git ssh && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN useradd -mU dbt_test_user
RUN mkdir /usr/app && chown dbt_test_user /usr/app
RUN mkdir /home/tox && chown dbt_test_user /home/tox
USER dbt_test_user

WORKDIR /usr/app
VOLUME /usr/app

RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash

ENV PYENV_ROOT="/home/dbt_test_user/.pyenv" \
    PATH="/home/dbt_test_user/.pyenv/bin:/home/dbt_test_user/.pyenv/shims:$PATH"

RUN pyenv update && \
    echo "2.7.16 3.6.8 3.7.3" | xargs -P 4 -n 1 pyenv install && \
    pyenv global $(pyenv versions --bare)

RUN pyenv virtualenv 3.6.8 dbt36 && \
    pyenv virtualenv 3.7.3 dbt37 && \
    pyenv virtualenv 2.7.16 dbt27

RUN cd /usr/app && \
    python -m pip install -U pip && \
    python -m pip install tox && \
    pyenv local dbt37 && \
    python -m pip install -U pip && \
    python -m pip install tox && \
    pyenv local --unset && \
    pyenv local dbt36 && \
    python -m pip install -U pip && \
    python -m pip install tox && \
    pyenv local --unset && \
    pyenv local dbt27 && \
    python -m pip install -U pip && \
    python -m pip install tox && \
    pyenv local --unset && \
    pyenv rehash

RUN pyenv local dbt36 dbt37 dbt27

ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
