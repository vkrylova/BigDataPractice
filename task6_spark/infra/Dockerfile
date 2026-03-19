FROM apache/spark:4.1.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

USER spark
WORKDIR /opt/spark/jobs
