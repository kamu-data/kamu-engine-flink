ARG BASE_IMAGE
FROM $BASE_IMAGE
ARG TARGETPLATFORM

# Tools
RUN apt-get update && \
    apt-get install -y tini procps && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Config
ADD image/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
ADD image/log4j-cli.properties /opt/flink/conf/log4j-cli.properties
ADD image/docker-entrypoint.sh /docker-entrypoint.sh


# Engine
ADD target/scala-2.12/engine.flink.jar /opt/engine/bin/engine.flink.jar


# Adapter
COPY image/tmp/$TARGETPLATFORM/adapter /opt/engine/bin/adapter


EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--"]
ENV RUST_BACKTRACE=1
CMD ["/opt/engine/bin/adapter"]