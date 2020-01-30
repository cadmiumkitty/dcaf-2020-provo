FROM wurstmeister/kafka:2.12-2.4.0
COPY ./connect-standalone.properties /opt/kafka/config/connect-standalone.properties
COPY ./connect-file-sink.properties /opt/kafka/config/connect-file-sink.properties
ENTRYPOINT /opt/kafka/bin/connect-standalone.sh /opt/kafka/config/connect-standalone.properties /opt/kafka/config/connect-file-sink.properties
