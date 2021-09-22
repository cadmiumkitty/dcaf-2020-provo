FROM openjdk:13
ENV FUSEKI_HOME=/apache-jena-fuseki-4.2.0
ENV FUSEKI_BASE=/apache-jena-fuseki-4.2.0/run
RUN curl https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-4.2.0.tar.gz -o ./apache-jena-fuseki-4.2.0.tar.gz
RUN tar xvzf apache-jena-fuseki-4.2.0.tar.gz
RUN chmod +x $FUSEKI_HOME/fuseki-server
RUN mkdir -p $FUSEKI_HOME/dcaf
COPY ./shiro.ini $FUSEKI_BASE/shiro.ini
WORKDIR $FUSEKI_HOME
ENTRYPOINT ./fuseki-server --loc=$FUSEKI_HOME/dcaf --update /dcaf