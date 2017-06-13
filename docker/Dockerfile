FROM jfloff/alpine-python:3.4-slim

ENV DBT_VERSION=0.8.2

RUN apk add postgresql-dev gettext libffi-dev coreutils --update-cache \
  && /entrypoint.sh -p dbt==$DBT_VERSION \
  && mkdir -p /root/.dbt

WORKDIR /root

ADD ./docker-entrypoint.sh /root/docker-entrypoint.sh

ENTRYPOINT ["/root/docker-entrypoint.sh"]
