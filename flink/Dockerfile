# Flink with the Kafka SQL connector baked in, shared by the jobmanager and taskmanager.
# Baking it in (instead of downloading at container start) makes restarts fast and work offline.
ARG FLINK_VERSION=1.18.1
FROM flink:${FLINK_VERSION}-scala_2.12

# The connector version has to match the Flink version.  Releases are listed at
# https://central.sonatype.com/artifact/org.apache.flink/flink-sql-connector-kafka/versions
# This is flink-sql-connector-kafka as opposed to flink-connector-kafka because it bundles all of its dependencies.
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar
