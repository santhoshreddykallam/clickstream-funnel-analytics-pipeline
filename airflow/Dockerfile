FROM apache/airflow:2.8.1
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow
RUN pip install pyspark==3.5.3 psycopg2-binary pandas