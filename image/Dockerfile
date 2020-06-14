ARG BASE_IMAGE
FROM $BASE_IMAGE

ADD image/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
ADD image/docker-entrypoint.sh /docker-entrypoint.sh
ADD target/scala-2.12/engine.flink.jar /opt/engine/bin/engine.flink.jar
