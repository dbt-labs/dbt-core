FROM fishtownjacob/hadoop:latest

ENV HIVE_VERSION 2.3.4
ENV HIVE_HOME /opt/hive

RUN mkdir ${HIVE_HOME} && \
	curl http://apache.cs.utah.edu/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz -o hive.tar.gz && \
	tar -xf hive.tar.gz -C ${HIVE_HOME} --strip-components=1 && \
	rm hive.tar.gz

COPY conf/* ${HIVE_HOME}/conf/
# overwrite the core-site with one pointing to the hadoop server
COPY hadoop_conf/* ${HADOOP_CONF_DIR}/
COPY run_hiveserver.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/run_hiveserver.sh

ENV PATH ${PATH}:${HIVE_HOME}/bin

WORKDIR ${HIVE_HOME}

