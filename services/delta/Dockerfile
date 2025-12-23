FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Delta Lake
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

RUN pip3 install pyspark==3.5.1 delta-spark==3.1.0 jupyter notebook

ENV PYSPARK_PYTHON=python3
ENV JUPYTER_ENABLE_LAB=yes

WORKDIR /opt/spark/work-dir
