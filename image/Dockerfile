ARG BASE_IMAGE
FROM $BASE_IMAGE

# Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

# Config
ADD image/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
ADD image/log4j-cli.properties /opt/flink/conf/log4j-cli.properties
ADD image/docker-entrypoint.sh /docker-entrypoint.sh


# Engine
ADD target/scala-2.12/engine.flink.jar /opt/engine/bin/engine.flink.jar


# Adapter
COPY adapter/target/x86_64-unknown-linux-gnu/release/kamu-engine-spark-adapter /opt/engine/bin/adapter


EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/engine/bin/adapter"]