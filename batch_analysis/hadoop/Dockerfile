FROM apache/hadoop:3.3.6

USER root

ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig
RUN wget -q https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} ${PIG_HOME} && \
    rm pig-${PIG_VERSION}.tar.gz

ENV PATH=${PIG_HOME}/bin:$PATH
ENV PIG_CLASSPATH=${HADOOP_HOME}/etc/hadoop

RUN mkdir -p /pig_scripts /data /results

COPY config/* ${HADOOP_HOME}/etc/hadoop/
COPY init-hdfs.sh /opt/
RUN chmod +x /opt/init-hdfs.sh

WORKDIR /opt
