FROM fishtownjacob/hive:latest

ENV PRESTO_HOME /opt/presto
ENV PRESTO_VERSION 0.214

RUN apt-get update && \
  apt-get install -yf python && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir ${PRESTO_HOME} && \
  curl https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz -o presto-server.tar.gz && \
  tar -xf presto-server.tar.gz -C ${PRESTO_HOME} --strip-components=1 && \
  rm presto-server.tar.gz

COPY etc ${PRESTO_HOME}/etc
COPY run_presto.sh ${PRESTO_HOME}

EXPOSE 8080

CMD ["/opt/presto/bin/launcher", "run"]
