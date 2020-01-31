FROM wurstmeister/kafka:2.12-2.4.0
COPY ./connect-standalone.properties /opt/kafka/config/connect-standalone.properties
COPY ./connect-sparql-sink.properties /opt/kafka/config/connect-sparql-sink.properties
COPY ./target/connect-sparql-sink-0.0.1-SNAPSHOT-standalone.jar /kafka-connect-sparql/connect-sparql-sink-0.0.1-SNAPSHOT-standalone.jar
ENTRYPOINT /opt/kafka/bin/connect-standalone.sh /opt/kafka/config/connect-standalone.properties /opt/kafka/config/connect-sparql-sink.properties
