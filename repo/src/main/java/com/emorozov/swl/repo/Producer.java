package com.emorozov.swl.repo;

import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Activity;
import org.openprovenance.prov.model.Agent;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.Entity;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.model.Namespace;
import org.openprovenance.prov.model.QualifiedName;
import org.openprovenance.prov.model.WasAssociatedWith;
import org.openprovenance.prov.model.WasGeneratedBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.extern.java.Log;

import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;

@Component
@Log
public class Producer {

  @Value("repo.topic.repo")
  private String repoTopic;

  @Value("repo.topic.prov")
  private String provTopic;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private Namespace ns;

  @Autowired
  private ProvFactory provFactory;

  @Autowired
  private InteropFramework interopFramework;

  @Scheduled(fixedRate = 5000)
  public void sendMessage() {
    String repoEventId = UUID.randomUUID().toString();
    OffsetDateTime odt = OffsetDateTime.now();

    String repoEventMessage = createRepoEventMessage(repoEventId, odt);
    String repoEventProvenanceMessage = createRepoEventProvenanceMessage(repoEventId, odt);

    log.info(repoEventMessage);
    log.info(repoEventProvenanceMessage);

    this.kafkaTemplate.send(repoTopic, repoEventMessage);
    this.kafkaTemplate.send(provTopic, repoEventProvenanceMessage);
  }

  private String createRepoEventMessage(String repoEventId, OffsetDateTime odt) {
    return String.format("Repo event: %s. Timestamp: %s", repoEventId, odt.toString());
  }

  private String createRepoEventProvenanceMessage(String repoEventId, OffsetDateTime odt) {

    QualifiedName eventQn = qn(repoEventId + "-event");
    QualifiedName activityQn = qn(repoEventId + "-activity");
    QualifiedName agentQn = qn("emorozov");

    Entity event = provFactory.newEntity(eventQn, "New or Correct");
    Activity activity = provFactory.newActivity(activityQn, "Creating new trade or correcting old one.");
    WasGeneratedBy wasGeneratedBy = provFactory.newWasGeneratedBy(null, eventQn, activityQn);
    
    Agent trader = provFactory.newAgent(agentQn, "Eugene Morozov");
    WasAssociatedWith wasAssociatedWith = provFactory.newWasAssociatedWith(null, eventQn, agentQn);

    Document repoEventProvDocument = provFactory.newDocument();
    repoEventProvDocument.getStatementOrBundle().addAll(
      Arrays.asList(event, activity, trader, wasGeneratedBy, wasAssociatedWith));
    repoEventProvDocument.setNamespace(ns);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    interopFramework.writeDocument(baos, Formats.ProvFormat.TURTLE, repoEventProvDocument);

    return String.format("Provenance record: %s", baos.toString());
  }

  public QualifiedName qn(String name) {
    return ns.qualifiedName("swl", name, provFactory);
  }
}