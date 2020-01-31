package com.emorozov.swl.connector.sparql;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpRdfUpdateSinkTask extends SinkTask {

  private String sparqlHttpEndpoint;

  public void start(Map<String, String> props) {
    this.sparqlHttpEndpoint = props.get(HttpRdfUpdateSinkConnector.SPARQL_HTTP_ENDPOINT);
  }

  @Override
  public void stop() {
  }

  @Override
  public void put(Collection<SinkRecord> records) {

    log.info("Got {} records to push.", records.size());

    for (SinkRecord record : records) {

      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        HttpPost post = new HttpPost(sparqlHttpEndpoint);
        post.setEntity(new StringEntity(record.value().toString()));
        post.setHeader("Content-Type", "text/turtle");

        HttpResponse response = httpClient.execute(post);
        log.info(response.toString());

      } catch (IOException ioe) {

        throw new RetriableException(ioe);
      }
    }
  }

  @Override
  public String version() {
    return "0.0.1";
  }
}
