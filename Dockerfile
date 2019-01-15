FROM python:3.6

# do these on one line so changes trigger apt-get update
RUN apt-get update && \
    apt-get install -y python-pip netcat python-dev python3-dev postgresql

RUN pip install pip --upgrade
RUN pip install virtualenv
RUN pip install virtualenvwrapper
RUN pip install tox

RUN useradd -mU dbt_test_user
USER dbt_test_user

WORKDIR /usr/src/app
RUN cd /usr/src/app
