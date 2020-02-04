package com.emorozov.swl.connector.sparql;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
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

    log.info("Got {} records to post.", records.size());

    for (SinkRecord record : records) {

      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        if (record.value() == null) {
          log.warn("Can not write null value record, skipping. Record: {}", record);
          return;
        }

        HttpPost post = new HttpPost(sparqlHttpEndpoint);
        post.setEntity(new StringEntity(record.value().toString()));
        post.setHeader("Content-Type", "text/turtle");

        HttpResponse response = httpClient.execute(post);
        log.info(response.toString());

        // Only include basic error handling, would need to implement retries, etc.
        // for the real sink.
        StatusLine statusLine = response.getStatusLine();
        if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
          throw new ConnectException(String.format("Received %s from SPARQL endpoint, discarding.", statusLine));
        }

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
