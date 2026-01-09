FROM apache/airflow:3.1.5-python3.12

# Switch to root to install OS packages
USER root

# Install Java (OpenJDK 17 is safest for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


# Adding Hadoop configs to trigger spark with master yarn
COPY tmp/hadoop-conf/ /opt/hadoop/etc/hadoop/

# Set HADOOP environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/opt/hadoop/etc/hadoop


# Switch back to airflow user
USER airflow
COPY tmp/ /tmp/
RUN pip install apache-airflow-providers-apache-kafka
RUN pip install /tmp/pyspark-3.5.1.tar.gz
RUN pip install apache-airflow-providers-apache-spark==4.1.0