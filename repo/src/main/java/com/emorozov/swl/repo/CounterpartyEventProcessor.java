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
import org.openprovenance.prov.model.SpecializationOf;
import org.openprovenance.prov.model.WasAssociatedWith;
import org.openprovenance.prov.model.WasDerivedFrom;
import org.openprovenance.prov.model.WasStartedBy;
import org.openprovenance.prov.model.WasEndedBy;
import org.openprovenance.prov.model.WasGeneratedBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.UUID;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

@Component
@Slf4j
public class CounterpartyEventProcessor {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private Namespace ns;

  @Autowired
  private ProvFactory provFactory;

  @Autowired
  private InteropFramework interopFramework;

  @Value("${repo.topic.counterparty-topic}")
  private String counterpartyTopic;

  @Value("${repo.topic.prov-topic}")
  private String provTopic;

  private String counterpartyId = UUID.randomUUID().toString();

  private int counterpartyVersionCounter = 5;

  @Scheduled(fixedRate = 30000, initialDelay = 15000)
  public void sendMessage() {

    String eventId = UUID.randomUUID().toString();
    OffsetDateTime odt = OffsetDateTime.now();

    int oldCounterpartyVersionNumber = counterpartyVersionCounter;
    int newCounterpartyVersionNumber = ++counterpartyVersionCounter;

    String counterpartyMessage = createCounterpartyFromCounterpartyEvent(counterpartyId, oldCounterpartyVersionNumber,
        newCounterpartyVersionNumber, eventId, odt);
    String provMessage = createProvCounterpartyEvent(counterpartyId, oldCounterpartyVersionNumber,
        newCounterpartyVersionNumber, eventId, odt);

    log.info(counterpartyMessage);
    log.info(provMessage);

    this.kafkaTemplate.send(counterpartyTopic, counterpartyMessage);
    this.kafkaTemplate.send(provTopic, provMessage);
  }

  private String createCounterpartyFromCounterpartyEvent(String counterpartyId, int oldCounterpartyVersionNumber,
      int newCounterpartyVersionNumber, String eventId, OffsetDateTime odt) {
    return String.format(
        "Counterparty credit rating update %s for counterparty %s at %s (version %s -> %s)",
        counterpartyId, eventId, odt.toString(), oldCounterpartyVersionNumber, newCounterpartyVersionNumber);
  }

  @SneakyThrows
  private String createProvCounterpartyEvent(String counterpartyId, int oldCounterpartyVersionNumber,
      int newCounterpartyVersionNumber, String eventId, OffsetDateTime odt) {

    QualifiedName counterpartyQn = qn(String.format("cpty-%s", counterpartyId));
    QualifiedName oldCounterpartyVersionQn = qn(
        String.format("cpty-%s-%s", counterpartyId, oldCounterpartyVersionNumber));
    QualifiedName newCounterpartyVersionQn = qn(
        String.format("cpty-%s-%s", counterpartyId, newCounterpartyVersionNumber));
    QualifiedName counterpartyUpdateQn = qn(String.format("update-%s", eventId));
    QualifiedName operationsQn = qn("michaelgray");
    QualifiedName counterpartyEventProcessorQn = qn("cpty-event-processor");

    Entity counterparty = provFactory.newEntity(counterpartyQn, "Bank X.");
    Entity newCounterpartyVersion = provFactory.newEntity(newCounterpartyVersionQn,
        String.format("Counterparty version %s", newCounterpartyVersionNumber));
    SpecializationOf newCounterpartyVersionSpecializationOf = provFactory.newSpecializationOf(newCounterpartyVersionQn,
        counterpartyQn);

    GregorianCalendar gcStartTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcStartTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcStartTime);
    GregorianCalendar gcEndTime = GregorianCalendar.from(odt.atZoneSameInstant(ZoneId.of("Z")));
    XMLGregorianCalendar xmlgcEndTime = DatatypeFactory.newInstance().newXMLGregorianCalendar(gcEndTime);

    Activity counterpartyUpdate = provFactory.newActivity(counterpartyUpdateQn, xmlgcStartTime, xmlgcEndTime,
        Collections.emptyList());
    provFactory.addLabel(counterpartyUpdate, String.format("Counterparty credit rating update on %s", odt.toString()));

    WasGeneratedBy newCounterpartyVersionWasGeneratedBy = provFactory.newWasGeneratedBy(null, newCounterpartyVersionQn,
        counterpartyUpdateQn);

    Agent operations = provFactory.newAgent(operationsQn, "Michael Gray");
    WasStartedBy wasStartedBy = provFactory.newWasStartedBy(null, counterpartyUpdateQn, operationsQn);
    WasEndedBy wasEndedBy = provFactory.newWasEndedBy(null, counterpartyUpdateQn, operationsQn);

    Agent counterpartyEventProcessor = provFactory.newAgent(counterpartyEventProcessorQn,
        "Counterparty Event Processor");
    WasAssociatedWith wasAssociatedWith = provFactory.newWasAssociatedWith(null, counterpartyUpdateQn,
        counterpartyEventProcessorQn);

    WasDerivedFrom counterpartyVersionDerivation = provFactory.newWasDerivedFrom(null, newCounterpartyVersionQn,
        oldCounterpartyVersionQn);

    Document repoEventProvDocument = provFactory.newDocument();
    repoEventProvDocument.getStatementOrBundle()
        .addAll(Arrays.asList(counterparty, newCounterpartyVersion, newCounterpartyVersionSpecializationOf,
            counterpartyUpdate, newCounterpartyVersionWasGeneratedBy, operations, wasStartedBy, wasEndedBy,
            counterpartyEventProcessor, wasAssociatedWith, counterpartyVersionDerivation));
    repoEventProvDocument.setNamespace(ns);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    interopFramework.writeDocument(baos, Formats.ProvFormat.TURTLE, repoEventProvDocument);

    return baos.toString();
  }

  public QualifiedName qn(String name) {
    return ns.qualifiedName(RepoConfiguration.SWL_PREFIX, name, provFactory);
  }
}