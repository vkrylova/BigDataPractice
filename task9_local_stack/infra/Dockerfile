FROM apache/spark:4.1.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark

USER spark
WORKDIR /opt/spark/jobs