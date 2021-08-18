ARG BASE_IMAGE=python:3.8-slim-bullseye

FROM $BASE_IMAGE
ARG BASE_REQUIREMENTS_SRC_PATH
ARG WHEEL_REQUIREMENTS_SRC_PATH
ARG DIST_PATH

# We need backport packages to get a more recent version of git
RUN printf "deb http://deb.debian.org/debian buster-backports main" > /etc/apt/sources.list.d/backports_git.list && \
    apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    git-man/buster-backports \
    git/buster-backports \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN echo BASE_REQUIREMENTS_SRC_PATH=$BASE_REQUIREMENTS_SRC_PATH
RUN echo WHEEL_REQUIREMENTS_SRC_PATH=$WHEEL_REQUIREMENTS_SRC_PATH
RUN echo DIST_PATH=$DIST_PATH
COPY $BASE_REQUIREMENTS_SRC_PATH ./requirements.txt
COPY $WHEEL_REQUIREMENTS_SRC_PATH ./wheel_requirements.txt
COPY $DIST_PATH ./dist
RUN pip install --upgrade pip setuptools
RUN pip install --requirement ./requirements.txt
RUN pip install --requirement ./wheel_requirements.txt
ENV PYTHONIOENCODING=utf-8
ENV LANG C.UTF-8
WORKDIR /usr/app
VOLUME /usr/app
ENTRYPOINT ["dbt"]
