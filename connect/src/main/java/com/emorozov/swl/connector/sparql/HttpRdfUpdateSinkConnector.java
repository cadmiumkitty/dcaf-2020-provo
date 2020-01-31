package com.emorozov.swl.connector.sparql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpRdfUpdateSinkConnector extends SinkConnector {

  public static final String TOPIC_CONFIG = "topics";
  public static final String SPARQL_HTTP_ENDPOINT = "sparql.http.endpoint";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH,
          "The topic to read data from. Expect raw RDF data in Turtle encoding")
      .define(SPARQL_HTTP_ENDPOINT, Type.STRING, Importance.HIGH, "SPARQL 1.1 HTTP Endpoint, typically '/data?default'");

  private String topic;

  private String sparqlHttpEndpoint;

  @Override
  public void start(final Map<String, String> props) {
    topic = props.get(TOPIC_CONFIG);
    sparqlHttpEndpoint = props.get(SPARQL_HTTP_ENDPOINT);
  }

  @Override
  public void stop() {
  }

  @Override
  public Class<? extends Task> taskClass() {
    return HttpRdfUpdateSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>();
    config.put(TOPIC_CONFIG, topic);
    config.put(SPARQL_HTTP_ENDPOINT, sparqlHttpEndpoint);
    configs.add(config);
    return configs;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public String version() {
    return "0.0.1";
  }
}