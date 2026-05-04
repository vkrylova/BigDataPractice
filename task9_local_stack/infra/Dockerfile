FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir "pyspark[connect]==3.5.1"

USER spark

# Start Spark Connect Server with Hadoop configs injected directly
CMD ["/opt/spark/bin/spark-submit", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--conf", "spark.hadoop.fs.s3a.endpoint=http://localstack-main:4566", \
     "--conf", "spark.hadoop.fs.s3a.access.key=test", \
     "--conf", "spark.hadoop.fs.s3a.secret.key=test", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false", \
     "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", \
     "--packages", "org.apache.spark:spark-connect_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "--class", "org.apache.spark.sql.connect.service.SparkConnectServer", \
     "--name", "Spark Connect Server"]