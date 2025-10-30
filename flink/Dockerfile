FROM flink:1.17.0-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.1

COPY flink-sql-connector-kafka-1.17.2.jar /opt/flink/lib/

WORKDIR /app